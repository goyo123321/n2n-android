package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// PeerInfo 对端信息
type PeerInfo struct {
	ClientID      string
	VirtualIP     string
	PubIP         string
	PubPort       int
	SharePort     int
	TurnRelayAddr string
	UDPAddr       *net.UDPAddr
	lastRecvAt    int64
}

// PeerSnapshot UI 用
type PeerSnapshot struct {
	Code      string `json:"code"`
	ClientID  string `json:"clientId"`
	VirtualIP string `json:"vip"`
	Online    bool   `json:"online"`
	SharePort int    `json:"sharePort"`
	ConnType  string `json:"connType"`
	NATType   string `json:"natType"`
}

// Edge 主结构
type Edge struct {
	cfg       *Config
	clientId  string
	nodeName  string
	virtualIP string
	roomId    string

	ws         *WSTransport
	relayMgr   *RelayManager
	turnClient *TURNClient
	tun        *TUNDevice

	udpConn *net.UDPConn
	udpPort int

	peers   map[string]*PeerInfo
	peersMu sync.RWMutex

	tunWriteCh chan []byte
	natMeta    *NATMetadata

	netstack *NetstackHost
	progress *ProgressDispatcher
	wsOutbound *WSOutbound
	httpProxy  *HTTPProxy

	doneCh  chan struct{}
	closeMu sync.Mutex
	closed  bool
	mu      sync.Mutex
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

// generateDefaultClientID 生成默认 Client ID
func generateDefaultClientID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "android"
	}
	return fmt.Sprintf("%s-android", hostname)
}

// ============ 提前获取虚拟 IP ============

// FetchVirtualIP 连一次信令拿虚拟 IP，然后断开
func FetchVirtualIP(cfg *Config) string {
	if cfg.SignalingURL == "" {
		return ""
	}

	clientId := cfg.ClientID
	if clientId == "" {
		clientId = generateDefaultClientID()
	}

	ws, err := NewWSTransport(
		cfg.SignalingURL, cfg.RoomID, clientId, cfg.ConnectToken,
		cfg.PreferredIP, cfg.PreferredPort,
	)
	if err != nil {
		log.Printf("[FetchVIP] 连接信令失败: %v", err)
		return ""
	}
	defer ws.Close()

	done := make(chan string, 1)
	ws.onMessage = func(msg map[string]interface{}) {
		t, _ := msg["type"].(string)
		if t == "ready" {
			payload, _ := msg["payload"].(map[string]interface{})
			vip, _ := payload["virtualIp"].(string)
			select {
			case done <- vip:
			default:
			}
		}
	}

	select {
	case vip := <-done:
		log.Printf("[FetchVIP] 拿到虚拟 IP: %s", vip)
		return vip
	case <-time.After(15 * time.Second):
		log.Printf("[FetchVIP] 超时")
		return ""
	}
}

// Start 启动客户端
func Start(cfg *Config, tunFd int) (*Edge, error) {
	if cfg.SignalingURL == "" {
		return nil, fmt.Errorf("signaling URL 为空")
	}
	if tunFd <= 0 {
		return nil, fmt.Errorf("TUN fd 无效")
	}

	clientId := cfg.ClientID
	if clientId == "" {
		clientId = generateDefaultClientID()
	}
	nodeName := cfg.NodeName
	if nodeName == "" {
		nodeName = "Android"
	}

	e := &Edge{
		cfg:        cfg,
		clientId:   clientId,
		nodeName:   nodeName,
		roomId:     cfg.RoomID,
		peers:      make(map[string]*PeerInfo),
		tunWriteCh: make(chan []byte, 1024),
		udpPort:    50001,
		doneCh:     make(chan struct{}),
	}

	// 1. 包装 TUN
	tun, err := setupTUNFromFD(tunFd)
	if err != nil {
		return nil, fmt.Errorf("包装 TUN fd 失败: %w", err)
	}
	e.tun = tun

	// 2. UDP 监听
	udpAddr := &net.UDPAddr{IP: net.IPv4zero, Port: e.udpPort}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("绑定 UDP %d 失败: %w", e.udpPort, err)
	}
	e.udpConn = udpConn

	// 3. NAT 探测（IPv4-only）
	e.natMeta = probeNAT(e.udpPort, nil)

	// 4. 连接信令（支持优选 IP）
	ws, err := NewWSTransport(
		cfg.SignalingURL, cfg.RoomID, clientId, cfg.ConnectToken,
		cfg.PreferredIP, cfg.PreferredPort,
	)
	if err != nil {
		udpConn.Close()
		return nil, fmt.Errorf("连接信令失败: %w", err)
	}
	e.ws = ws

	// 5. TURN
	e.turnClient = NewTURNClient(cfg.SignalingURL, cfg.ConnectToken, e)
	e.turnClient.onMessage = func(data []byte, addr net.Addr) {
		e.onRemotePacket(data)
	}

	// 6. 中继
	e.relayMgr = NewRelayManager(ws, e.turnClient, e)
	e.relayMgr.Report(10 * time.Second)

	// 7. 回调
	ws.onMessage = e.handleSignaling
	ws.onBinary = func(data []byte) {
		e.onRemotePacket(data)
	}

	// 8. 后台协程
	go e.udpReadLoop()
	go e.tunWriteLoop()
	go e.tunReadLoop()

	// 9. progress 空初始化
	e.progress = &ProgressDispatcher{}

	// 10. TURN + WSOutbound 异步初始化
	go func() {
		time.Sleep(2 * time.Second)
		ctx, cancel := contextWithTimeout(30 * time.Second)
		defer cancel()

		if err := e.turnClient.FetchAndSetup(ctx); err != nil {
			log.Printf("[TURN] 初始化失败: %v", err)
		} else {
			log.Printf("[TURN] 就绪: %s", e.turnClient.GetRelayAddr())
			e.relayMgr.UpgradeRelaysToTURN()
			_ = e.ws.Send(map[string]interface{}{
				"type":      "turn_relay_info",
				"relayAddr": e.turnClient.GetRelayAddr(),
			})
		}

		// Workers 出口
		wsOut, err := NewWSOutbound(
			cfg.SignalingURL, cfg.RoomID, clientId, cfg.ConnectToken,
			cfg.PreferredIP, cfg.PreferredPort,
		)
		if err != nil {
			log.Printf("[WSOut] 初始化失败: %v", err)
			return
		}
		e.mu.Lock()
		e.wsOutbound = wsOut
		e.mu.Unlock()
		log.Printf("[WSOut] Workers 出口代理就绪")
	}()

	log.Printf("[Edge] 已启动 clientId=%s room=%s", clientId, cfg.RoomID)
	return e, nil
}

// Stop 停止
func (e *Edge) Stop() {
	e.closeMu.Lock()
	if e.closed {
		e.closeMu.Unlock()
		return
	}
	e.closed = true
	close(e.doneCh)
	e.closeMu.Unlock()

	if e.ws != nil {
		_ = e.ws.Close()
	}
	if e.turnClient != nil {
		e.turnClient.Close()
	}
	if e.udpConn != nil {
		_ = e.udpConn.Close()
	}
	if e.tun != nil {
		_ = e.tun.Close()
	}
	if e.wsOutbound != nil {
		e.wsOutbound.Close()
	}
	if e.httpProxy != nil {
		e.httpProxy.Close()
	}
	log.Printf("[Edge] 已停止")
}

// Done 关闭信号
func (e *Edge) Done() <-chan struct{} {
	return e.doneCh
}

// GetVirtualIP 虚拟 IP
func (e *Edge) GetVirtualIP() string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	return e.virtualIP
}

// GetClientID 客户端 ID
func (e *Edge) GetClientID() string {
	return e.clientId
}

// GetPeersJSON 节点列表 JSON
func (e *Edge) GetPeersJSON() string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()

	type pair struct {
		id string
		p  *PeerInfo
	}
	var list []pair
	for id, p := range e.peers {
		list = append(list, pair{id, p})
	}
	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[i].id > list[j].id {
				list[i], list[j] = list[j], list[i]
			}
		}
	}

	var snapshots []PeerSnapshot
	for i, item := range list {
		code := idxToCode(i)
		connType := string(e.relayMgr.GetState(item.id))
		snapshots = append(snapshots, PeerSnapshot{
			Code:      code,
			ClientID:  item.id,
			VirtualIP: item.p.VirtualIP,
			Online:    item.p.UDPAddr != nil || item.p.TurnRelayAddr != "",
			SharePort: 9090,
			ConnType:  connType,
			NATType:   e.natMeta.NATType,
		})
	}

	data, _ := json.Marshal(snapshots)
	return string(data)
}

func idxToCode(i int) string {
	if i < 26 {
		return string(rune('A' + i))
	}
	first := i/26 - 1
	second := i % 26
	return string(rune('A'+first)) + string(rune('A'+second))
}

// ============ 信令消息处理 ============

func (e *Edge) handleSignaling(msg map[string]interface{}) {
	t, _ := msg["type"].(string)
	from, _ := msg["from"].(string)

	if t == "_reconnected" {
		log.Printf("[信令] WebSocket 重连成功，重新上报元数据")
		e.reportMetadata()
		return
	}

	switch t {
	case "ready":
		payload, _ := msg["payload"].(map[string]interface{})
		e.peersMu.Lock()
		e.virtualIP, _ = payload["virtualIp"].(string)
		vip := e.virtualIP
		e.peersMu.Unlock()
		log.Printf("[信令] 分配虚拟 IP: %s", vip)

		// ★ 兜底：STUN 失败时用服务端看到的公网 IP
		if e.natMeta.PublicEndpoint == "" {
			if serverSeenIp, ok := payload["yourPublicIp"].(string); ok && serverSeenIp != "" {
				endpoint := fmt.Sprintf("%s:%d", serverSeenIp, e.udpPort)
				e.natMeta.PublicEndpoint = endpoint
				e.natMeta.P2PEndpoint = endpoint
				e.natMeta.NATType = "EasyNAT"
				log.Printf("[NAT] STUN 失败，用服务端 IP 兜底: %s", endpoint)
			}
		}

		// 初始化 netstack + 共享盘
		if e.netstack == nil && vip != "" {
			ns, err := NewNetstackHost(vip)
			if err != nil {
				log.Printf("[Netstack] 初始化失败: %v", err)
			} else {
				ns.progress = e.progress
				e.netstack = ns
				go e.netstackReadLoop()
				if e.cfg != nil && e.cfg.ShareDir != "" {
					if err := ns.StartShareServer(e.cfg.ShareDir); err != nil {
						log.Printf("[Netstack] 共享盘启动失败: %v", err)
					}
				}
			}
		}

		// 上报元数据
		e.reportMetadata()

		// ★ 打印已有节点列表（诊断）
		if peers, ok := payload["peers"].([]interface{}); ok {
			log.Printf("[信令] ready: 返回 %d 个已有节点", len(peers))
			for _, p := range peers {
				pm, ok := p.(map[string]interface{})
				if !ok {
					continue
				}
				pid, _ := pm["id"].(string)
				pip, _ := pm["virtualIp"].(string)
				pubIP, _ := pm["publicIp"].(string)
				pubPort := jsonInt(pm["publicPort"])
				sharePort := jsonInt(pm["sharePort"])
				relayAddr, _ := pm["turnRelayAddr"].(string)
				if pid == "" || pip == "" {
					continue
				}
				e.registerPeer(pid, pip, pubIP, pubPort, sharePort)
				if relayAddr != "" {
					e.peersMu.Lock()
					if pi, ok := e.peers[pid]; ok {
						pi.TurnRelayAddr = relayAddr
					}
					e.peersMu.Unlock()
				}
				log.Printf("[信令] 已有节点: %s vip=%s pub=%s:%d share=%d",
					pid, pip, pubIP, pubPort, sharePort)
			}
		} else {
			log.Printf("[信令] ready: 返回 0 个已有节点")
		}

	case "joined":
		payload, _ := msg["payload"].(map[string]interface{})
		if payload == nil {
			log.Printf("[信令] joined: payload 为空，忽略")
			return
		}
		pip, _ := payload["virtualIp"].(string)
		pubIP, _ := payload["publicIp"].(string)
		pubPort := jsonInt(payload["publicPort"])
		sharePort := jsonInt(payload["sharePort"])
		relayAddr, _ := payload["turnRelayAddr"].(string)

		log.Printf("[信令] joined: from=%s vip=%s pub=%s:%d", from, pip, pubIP, pubPort)

		if from != "" && pip != "" {
			e.registerPeer(from, pip, pubIP, pubPort, sharePort)
			if relayAddr != "" {
				e.peersMu.Lock()
				if pi, ok := e.peers[from]; ok {
					pi.TurnRelayAddr = relayAddr
				}
				e.peersMu.Unlock()
			}
			log.Printf("[信令] 节点上线: %s vip=%s", from, pip)
		}

	case "nat_hole_instruction":
		raw, _ := json.Marshal(msg["payload"])
		var instr NatHoleInstruction
		if err := json.Unmarshal(raw, &instr); err != nil {
			return
		}
		e.ensureTargetPeer(&instr)
		go e.runNatHole(&instr)

	case "turn_peer_info":
		edgeMac, _ := msg["edgeMac"].(string)
		relayAddr, _ := msg["relayAddr"].(string)
		if edgeMac != "" && relayAddr != "" {
			e.peersMu.Lock()
			if p, ok := e.peers[edgeMac]; ok {
				p.TurnRelayAddr = relayAddr
			}
			e.peersMu.Unlock()
		}

	case "pong":
		return

	case "left":
		log.Printf("[信令] 节点离开: %s", from)
		e.peersMu.Lock()
		delete(e.peers, from)
		e.peersMu.Unlock()

	case "connection_status":
		return
	}
}

// reportMetadata 上报 p2p_metadata 和 share_announce
func (e *Edge) reportMetadata() {
	metaPayload := map[string]interface{}{
		"natType":            e.natMeta.NATType,
		"portsDifference":    e.natMeta.PortsDifference,
		"regularPortsChange": e.natMeta.RegularPortsChange,
		"behavior":           e.natMeta.Behavior,
		"assistedSockets":    e.natMeta.AssistedSockets,
		"sharePort":          9090,
		"p2pEndpoint":        e.natMeta.P2PEndpoint,
	}
	if e.natMeta.PublicEndpoint != "" {
		metaPayload["publicEndpoint"] = e.natMeta.PublicEndpoint
	}
	_ = e.ws.Send(map[string]interface{}{
		"type":    "p2p_metadata",
		"payload": metaPayload,
	})

	_ = e.ws.Send(map[string]interface{}{
		"type": "share_announce",
		"payload": map[string]interface{}{
			"name":      e.nodeName,
			"virtualIp": e.GetVirtualIP(),
			"port":      9090,
		},
	})
}

func jsonInt(v interface{}) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

func (e *Edge) registerPeer(pid, vip, pubIP string, pubPort int, sharePort int) {
	var udpAddr *net.UDPAddr
	if pubIP != "" && pubPort > 0 {
		udpAddr = &net.UDPAddr{IP: net.ParseIP(pubIP), Port: pubPort}
	}
	e.peersMu.Lock()
	defer e.peersMu.Unlock()
	if p, ok := e.peers[pid]; ok {
		if vip != "" {
			p.VirtualIP = vip
		}
		if pubIP != "" {
			p.PubIP = pubIP
		}
		if pubPort > 0 {
			p.PubPort = pubPort
		}
		if sharePort > 0 {
			p.SharePort = sharePort
		}
		if udpAddr != nil {
			p.UDPAddr = udpAddr
		}
	} else {
		e.peers[pid] = &PeerInfo{
			ClientID:  pid,
			VirtualIP: vip,
			PubIP:     pubIP,
			PubPort:   pubPort,
			SharePort: sharePort,
			UDPAddr:   udpAddr,
		}
	}
}

func (e *Edge) ensureTargetPeer(instr *NatHoleInstruction) {
	if instr.TargetMac == "" {
		return
	}
	var udpAddr *net.UDPAddr
	if instr.TargetPubSocket != "" {
		udpAddr = parseSockAddr(instr.TargetPubSocket)
	}
	e.peersMu.Lock()
	defer e.peersMu.Unlock()
	if p, ok := e.peers[instr.TargetMac]; ok {
		if udpAddr != nil {
			p.UDPAddr = udpAddr
		}
		if instr.TargetVirtualIp != "" {
			p.VirtualIP = instr.TargetVirtualIp
		}
	} else {
		e.peers[instr.TargetMac] = &PeerInfo{
			ClientID:  instr.TargetMac,
			VirtualIP: instr.TargetVirtualIp,
			UDPAddr:   udpAddr,
		}
	}
}

func (e *Edge) runNatHole(instr *NatHoleInstruction) {
	res := e.executeNatHole(instr)

	_ = e.ws.Send(map[string]interface{}{
		"type": "p2p_state_info",
		"payload": map[string]interface{}{
			"to": []map[string]interface{}{
				{
					"macAddr":            instr.TargetMac,
					"observedRaddr":      "",
					"punchResult":        res,
					"punchResultPeerMac": instr.TargetMac,
				},
			},
		},
	})

	if res.State == PunchStateSucceeded {
		e.relayMgr.MarkP2P(instr.TargetMac)
	} else {
		e.relayMgr.MarkFallback(instr.TargetMac)
	}
}

// ============ 网络 IO 循环 ============

func (e *Edge) udpReadLoop() {
	buf := make([]byte, 65535)
	for {
		n, addr, err := e.udpConn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-e.doneCh:
				return
			default:
			}
			return
		}
		if n < 4 {
			continue
		}
		if buf[0] == 'N' && buf[1] == '2' && buf[2] == 'N' && buf[3] == 'P' {
			e.notePeerTraffic(addr)
			continue
		}
		if buf[0]>>4 == 4 {
			e.notePeerTraffic(addr)
			e.onRemotePacket(buf[:n])
		}
	}
}

func (e *Edge) tunReadLoop() {
	buf := make([]byte, 65535)
	for {
		n, err := e.tun.Read(buf)
		if err != nil {
			select {
			case <-e.doneCh:
				return
			default:
			}
			return
		}
		if n < 20 || buf[0]>>4 != 4 {
			continue
		}

		dstIP := net.IP(buf[16:20]).String()
		vip := e.GetVirtualIP()

		if dstIP == vip && e.netstack != nil {
			e.netstack.InjectTUNPacket(buf[:n])
			continue
		}

		e.peersMu.RLock()
		var target *PeerInfo
		for _, p := range e.peers {
			if p.VirtualIP == dstIP {
				target = p
				break
			}
		}
		e.peersMu.RUnlock()

		if target == nil {
			_ = e.ws.SendBinary(buf[:n])
			continue
		}
		if !e.relayMgr.SendToPeer(target.ClientID, buf[:n], target) {
			_ = e.ws.SendBinary(buf[:n])
		}
	}
}

func (e *Edge) tunWriteLoop() {
	for {
		select {
		case <-e.doneCh:
			return
		case data := <-e.tunWriteCh:
			if e.tun != nil {
				_, _ = e.tun.Write(data)
			}
		}
	}
}

func (e *Edge) netstackReadLoop() {
	for {
		select {
		case <-e.doneCh:
			return
		default:
		}
		if e.netstack == nil {
			return
		}
		data, ok := e.netstack.ReadTUNPacket()
		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}
		e.forwardPacketToPeer(data)
	}
}

func (e *Edge) forwardPacketToPeer(data []byte) {
	if len(data) < 20 || data[0]>>4 != 4 {
		return
	}
	dstIP := net.IP(data[16:20]).String()

	e.peersMu.RLock()
	var target *PeerInfo
	for _, p := range e.peers {
		if p.VirtualIP == dstIP {
			target = p
			break
		}
	}
	e.peersMu.RUnlock()

	if target == nil {
		_ = e.ws.SendBinary(data)
		return
	}
	if !e.relayMgr.SendToPeer(target.ClientID, data, target) {
		_ = e.ws.SendBinary(data)
	}
}

func (e *Edge) enqueueTUN(data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case e.tunWriteCh <- cp:
	default:
	}
}

func (e *Edge) onRemotePacket(data []byte) {
	if len(data) < 20 || data[0]>>4 != 4 {
		return
	}

	dstIP := net.IP(data[16:20]).String()
	vip := e.GetVirtualIP()

	if dstIP == vip && e.netstack != nil {
		e.netstack.InjectTUNPacket(data)
		return
	}
	if dstIP != vip {
		return
	}
	e.enqueueTUN(data)
}

func (e *Edge) notePeerTraffic(addr *net.UDPAddr) {
	now := time.Now().UnixMilli()
	e.peersMu.Lock()
	var best *PeerInfo
	bestScore := -1
	for _, p := range e.peers {
		if p.UDPAddr == nil || !p.UDPAddr.IP.Equal(addr.IP) {
			continue
		}
		score := 1
		if p.UDPAddr.Port == addr.Port {
			score = 2
		}
		if score > bestScore {
			best = p
			bestScore = score
		}
	}
	if best == nil {
		e.peersMu.Unlock()
		return
	}
	best.lastRecvAt = now
	if bestScore == 1 {
		best.UDPAddr = &net.UDPAddr{IP: addr.IP, Port: addr.Port}
	}
	clientID := best.ClientID
	e.peersMu.Unlock()

	if e.relayMgr.ShouldRelay(clientID) {
		log.Printf("[P2P] 从 %s 收到 UDP 包，升级为 P2P", clientID)
		e.relayMgr.MarkP2P(clientID)
	}
}
