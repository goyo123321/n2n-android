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

	ws          *WSTransport
	relayMgr    *RelayManager
	turnClient  *TURNClient
	tun         *TUNDevice

	udpConn *net.UDPConn
	udpPort int

	peers   map[string]*PeerInfo
	peersMu sync.RWMutex

	tunWriteCh chan []byte
	natMeta    *NATMetadata

	netstack *NetstackHost
	progress *ProgressDispatcher

	doneCh  chan struct{}
	closeMu sync.Mutex
	closed  bool
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

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
		hostname, _ := os.Hostname()
		clientId = fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%1e9)
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

	// 3. NAT 探测
	e.natMeta = probeNAT(e.udpPort, nil)

	// 4. 连接信令
	ws, err := NewWSTransport(cfg.SignalingURL, cfg.RoomID, clientId, cfg.ConnectToken)
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

	// 10. TURN 异步初始化
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

// GetPeersJSON 返回节点列表 JSON
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
	// 简单排序
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

	switch t {
	case "ready":
		payload, _ := msg["payload"].(map[string]interface{})
		e.peersMu.Lock()
		e.virtualIP, _ = payload["virtualIp"].(string)
		vip := e.virtualIP
		e.peersMu.Unlock()
		log.Printf("[信令] 分配虚拟 IP: %s", vip)

		// ★ 初始化 netstack + 共享盘
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
				"virtualIp": e.virtualIP,
				"port":      9090,
			},
		})

		// 处理已有 peers
		if peers, ok := payload["peers"].([]interface{}); ok {
			for _, p := range peers {
				pm, ok := p.(map[string]interface{})
				if !ok {
					continue
				}
				pid, _ := pm["id"].(string)
				pip, _ := pm["virtualIp"].(string)
				pubIP, _ := pm["publicIp"].(string)
				pubPort := jsonInt(pm["publicPort"])
				relayAddr, _ := pm["turnRelayAddr"].(string)
				if pid == "" || pip == "" {
					continue
				}
				e.registerPeer(pid, pip, pubIP, pubPort, relayAddr)
			}
		}

	case "joined":
		payload, _ := msg["payload"].(map[string]interface{})
		if payload == nil {
			return
		}
		pip, _ := payload["virtualIp"].(string)
		pubIP, _ := payload["publicIp"].(string)
		pubPort := jsonInt(payload["publicPort"])
		relayAddr, _ := payload["turnRelayAddr"].(string)
		if from != "" && pip != "" {
			e.registerPeer(from, pip, pubIP, pubPort, relayAddr)
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
		e.peersMu.Lock()
		delete(e.peers, from)
		e.peersMu.Unlock()

	case "connection_status":
		return
	}
}

func jsonInt(v interface{}) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

func (e *Edge) registerPeer(pid, vip, pubIP string, pubPort int, relayAddr string) {
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
		if udpAddr != nil {
			p.UDPAddr = udpAddr
		}
		if relayAddr != "" {
			p.TurnRelayAddr = relayAddr
		}
	} else {
		e.peers[pid] = &PeerInfo{
			ClientID:      pid,
			VirtualIP:     vip,
			PubIP:         pubIP,
			PubPort:       pubPort,
			UDPAddr:       udpAddr,
			TurnRelayAddr: relayAddr,
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
		// 打洞探测包
		if buf[0] == 'N' && buf[1] == '2' && buf[2] == 'N' && buf[3] == 'P' {
			e.notePeerTraffic(addr)
			continue
		}
		// IPv4
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

		// ★ 目标是本机 → 交给 netstack
		if dstIP == vip && e.netstack != nil {
			e.netstack.InjectTUNPacket(buf[:n])
			continue
		}

		// 目标是别的 peer → 转发
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

// onRemotePacket 处理从对端收到的包
func (e *Edge) onRemotePacket(data []byte) {
	if len(data) < 20 || data[0]>>4 != 4 {
		return
	}

	dstIP := net.IP(data[16:20]).String()
	vip := e.GetVirtualIP()

	// ★ 目标是本机 → netstack
	if dstIP == vip && e.netstack != nil {
		e.netstack.InjectTUNPacket(data)
		return
	}

	// 不是本机的包丢弃（避免死循环）
	if dstIP != vip {
		return
	}

	// netstack 未就绪 → 写回 TUN 兜底
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
