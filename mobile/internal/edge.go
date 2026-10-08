package internal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type PeerInfo struct {
	ClientID      string
	VirtualIP     string
	PubIP         string
	PubPort       int
	LanIP         string // ★ 新增：对端的局域网 IP
	LanPort       int    // ★ 新增：对端的本地 UDP 端口
	TurnRelayAddr string
	UDPAddr       *net.UDPAddr
	lastRecvAt    int64
	loggedReady   bool
	hasRealData   bool
}

type PeerSnapshot struct {
	Code      string `json:"code"`
	ClientID  string `json:"clientId"`
	VirtualIP string `json:"vip"`
	Online    bool   `json:"online"`
	ConnType  string `json:"connType"`
	NATType   string `json:"natType"`
}

type Edge struct {
	cfg       *Config
	clientId  string
	nodeName  string
	virtualIP string
	roomId    string

	// ★ 本机局域网信息（LAN 直连用）
	myLanIP     string
	myGatewayIP string

	ws         *WSTransport
	relayMgr   *RelayManager
	turnClient *TURNClient
	tun        *TUNDevice

	udpConn net.PacketConn
	udpPort int

	peers   map[string]*PeerInfo
	peersMu sync.RWMutex

	tunWriteCh chan []byte
	natMeta    *NATMetadata

	doneCh  chan struct{}
	closeMu sync.Mutex
	closed  bool
	mu      sync.Mutex
}

func generateDefaultClientID() string {
	hostname, _ := os.Hostname()
	if hostname == "" || hostname == "localhost" {
		hostname = "android"
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint32(b[:], uint32(time.Now().UnixNano()))
	}
	return fmt.Sprintf("%s-%s", hostname, hex.EncodeToString(b[:]))
}

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
func Start(cfg *Config, tunFd int, udpFd int, stunFd int) (*Edge, error) {
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

	// ★ 先探测本机局域网信息（LAN 直连需要）
	netInfo := getNetInfo()

	e := &Edge{
		cfg:         cfg,
		clientId:    clientId,
		nodeName:    nodeName,
		roomId:      cfg.RoomID,
		myLanIP:     netInfo.LANIP,
		myGatewayIP: netInfo.GatewayIP,
		peers:       make(map[string]*PeerInfo),
		tunWriteCh:  make(chan []byte, 4096),
		doneCh:      make(chan struct{}),
		natMeta: &NATMetadata{
			NATType:  "unknown",
			Behavior: "BehaviorPortChanged",
		},
	}
	log.Printf("[LAN] 本机局域网: %s, 网关: %s", e.myLanIP, e.myGatewayIP)

	// 1. TUN
	tun, err := setupTUNFromFD(tunFd)
	if err != nil {
		return nil, fmt.Errorf("包装 TUN fd 失败: %w", err)
	}
	e.tun = tun

	// 2. UDP（P2P 打洞，protected）
	var udpConn net.PacketConn
	if udpFd > 0 {
		file := os.NewFile(uintptr(udpFd), "protected-udp")
		if file != nil {
			pc, err := net.FilePacketConn(file)
			_ = file.Close()
			if err == nil {
				udpConn = pc
				log.Printf("[P2P] 使用 protected UDP fd=%d（绕过 VPN）", udpFd)
			} else {
				log.Printf("[P2P] FilePacketConn 失败: %v，回退默认 socket", err)
			}
		}
	}
	if udpConn == nil {
		addr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
		u, err := net.ListenUDP("udp", addr)
		if err != nil {
			_ = tun.Close()
			return nil, fmt.Errorf("绑定 UDP 失败: %w", err)
		}
		udpConn = u
		log.Printf("[P2P] 使用默认 UDP socket（无保护）")
	}
	e.udpConn = udpConn

	if la := udpConn.LocalAddr(); la != nil {
		if ua, ok := la.(*net.UDPAddr); ok {
			e.udpPort = ua.Port
		}
	}
	log.Printf("[P2P] UDP 监听端口 %d", e.udpPort)

	// 3. STUN socket（protected）
	var stunConn net.PacketConn
	if stunFd > 0 {
		file := os.NewFile(uintptr(stunFd), "protected-stun")
		if file != nil {
			pc, err := net.FilePacketConn(file)
			_ = file.Close()
			if err == nil {
				stunConn = pc
				log.Printf("[NAT] 使用 protected STUN socket fd=%d", stunFd)
			} else {
				log.Printf("[NAT] STUN FilePacketConn 失败: %v", err)
			}
		}
	}
	if stunConn == nil {
		log.Printf("[NAT] 无 protected STUN socket，STUN 探测可能失败")
	}

	// 4. 信令
	ws, err := NewWSTransport(
		cfg.SignalingURL, cfg.RoomID, clientId, cfg.ConnectToken,
		cfg.PreferredIP, cfg.PreferredPort,
	)
	if err != nil {
		_ = udpConn.Close()
		if stunConn != nil {
			_ = stunConn.Close()
		}
		_ = tun.Close()
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

	// 9. NAT 探测（异步）
	go func() {
		log.Printf("[NAT] 开始异步 STUN 探测...")
		var meta *NATMetadata
		if stunConn != nil {
			meta = probeNATWithConn(stunConn, nil)
			_ = stunConn.Close()
		} else {
			meta = probeNAT(e.udpPort, nil)
		}

		e.mu.Lock()
		e.natMeta = meta
		wsRef := e.ws
		e.mu.Unlock()

		log.Printf("[NAT] 探测完成: %s pub=%s", meta.NATType, meta.PublicEndpoint)

		if wsRef != nil {
			e.reportMetadata()
			log.Printf("[NAT] 已重新上报 p2p_metadata")
		}
	}()

	// 10. TURN 异步初始化
	go func() {
		time.Sleep(200 * time.Millisecond)
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

func (e *Edge) Done() <-chan struct{} { return e.doneCh }

func (e *Edge) GetVirtualIP() string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	return e.virtualIP
}

func (e *Edge) GetClientID() string { return e.clientId }

func (e *Edge) GetPeersJSON() string {
	e.peersMu.RLock()
	type pair struct {
		id string
		p  *PeerInfo
	}
	var list []pair
	for id, p := range e.peers {
		list = append(list, pair{id, p})
	}
	e.peersMu.RUnlock()

	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[i].id > list[j].id {
				list[i], list[j] = list[j], list[i]
			}
		}
	}

	e.mu.Lock()
	natType := e.natMeta.NATType
	e.mu.Unlock()

	var snapshots []PeerSnapshot
	for i, item := range list {
		code := idxToCode(i)
		connType := string(e.relayMgr.GetState(item.id))
		snapshots = append(snapshots, PeerSnapshot{
			Code:      code,
			ClientID:  item.id,
			VirtualIP: item.p.VirtualIP,
			Online:    item.p.UDPAddr != nil || item.p.TurnRelayAddr != "",
			ConnType:  connType,
			NATType:   natType,
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

// ============ 信令 ============

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

		e.mu.Lock()
		needFallback := e.natMeta.PublicEndpoint == ""
		e.mu.Unlock()

		if needFallback {
			if serverSeenIp, ok := payload["yourPublicIp"].(string); ok && serverSeenIp != "" {
				endpoint := fmt.Sprintf("%s:%d", serverSeenIp, e.udpPort)
				e.mu.Lock()
				e.natMeta.PublicEndpoint = endpoint
				e.natMeta.P2PEndpoint = endpoint
				e.natMeta.NATType = "EasyNAT"
				e.mu.Unlock()
				log.Printf("[NAT] STUN 未完成，用服务端 IP 兜底: %s", endpoint)
			}
		}

		e.reportMetadata()

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
				lanIP, _ := pm["lanIp"].(string)
				lanPort := jsonInt(pm["lanPort"])
				relayAddr, _ := pm["turnRelayAddr"].(string)
				if pid == "" || pip == "" {
					continue
				}
				e.registerPeer(pid, pip, pubIP, pubPort, lanIP, lanPort)
				if relayAddr != "" {
					e.peersMu.Lock()
					if pi, ok := e.peers[pid]; ok {
						pi.TurnRelayAddr = relayAddr
					}
					e.peersMu.Unlock()
				}
				log.Printf("[信令] 已有节点: %s vip=%s pub=%s:%d lan=%s:%d",
					pid, pip, pubIP, pubPort, lanIP, lanPort)
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
		lanIP, _ := payload["lanIp"].(string)
		lanPort := jsonInt(payload["lanPort"])
		relayAddr, _ := payload["turnRelayAddr"].(string)

		log.Printf("[信令] joined: from=%s vip=%s pub=%s:%d lan=%s:%d",
			from, pip, pubIP, pubPort, lanIP, lanPort)

		if from != "" && pip != "" {
			e.registerPeer(from, pip, pubIP, pubPort, lanIP, lanPort)
			if relayAddr != "" {
				e.peersMu.Lock()
				if pi, ok := e.peers[from]; ok {
					pi.TurnRelayAddr = relayAddr
				}
				e.peersMu.Unlock()
			}
		}

	case "nat_hole_instruction":
		raw, _ := json.Marshal(msg["payload"])
		var instr NatHoleInstruction
		if err := json.Unmarshal(raw, &instr); err != nil {
			log.Printf("[NAT-HOLE] 指令解析失败: %v", err)
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

// reportMetadata 上报 p2p_metadata + share_announce。
//
// 现在会额外上报 lanIp / udpPort——服务端广播 joined 时把这些字段
// 带给同房间的其他 peer，对方据此判断是否同局域网，若同网段可直接
// 走局域网 IP，延迟 < 5ms，无需 STUN / TURN。
func (e *Edge) reportMetadata() {
	e.mu.Lock()
	nm := e.natMeta
	ws := e.ws
	e.mu.Unlock()

	metaPayload := map[string]interface{}{
		"name":               e.nodeName,
		"natType":            nm.NATType,
		"portsDifference":    nm.PortsDifference,
		"regularPortsChange": nm.RegularPortsChange,
		"behavior":           nm.Behavior,
		"assistedSockets":    nm.AssistedSockets,
		"p2pEndpoint":        nm.P2PEndpoint,
		// ★ LAN 直连字段
		"lanIp":   e.myLanIP,
		"udpPort": e.udpPort,
	}
	if nm.PublicEndpoint != "" {
		metaPayload["publicEndpoint"] = nm.PublicEndpoint
	}

	log.Printf("[信令] 上报 p2p_metadata: natType=%s publicEndpoint=%s lanIp=%s udpPort=%d",
		nm.NATType, nm.PublicEndpoint, e.myLanIP, e.udpPort)

	if ws == nil {
		return
	}
	_ = ws.Send(map[string]interface{}{"type": "p2p_metadata", "payload": metaPayload})

	vip := e.GetVirtualIP()
	_ = ws.Send(map[string]interface{}{
		"type": "share_announce",
		"payload": map[string]interface{}{
			"name": e.nodeName, "virtualIp": vip, "port": 0,
		},
	})
}

func jsonInt(v interface{}) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

// registerPeer 注册或更新对端。
//
// ★ LAN 直连检测：
//   若对端上报的 lanIp 与本机 lanIp 在同一 /24 子网，直接用局域网地址
//   作为 UDPAddr，并标记 P2P。同 WiFi 下延迟 < 5ms，无需 STUN/TURN。
//   若 AP 隔离挡住，后续发送失败会自然降级到 TURN / WS。
func (e *Edge) registerPeer(pid, vip, pubIP string, pubPort int, lanIP string, lanPort int) {
	var udpAddr *net.UDPAddr
	if pubIP != "" && pubPort > 0 {
		udpAddr = &net.UDPAddr{IP: net.ParseIP(pubIP), Port: pubPort}
	}

	// ★ 同网段检测
	lanPreferred := false
	if lanIP != "" && lanPort > 0 && e.myLanIP != "" && sameSubnet(e.myLanIP, lanIP) {
		udpAddr = &net.UDPAddr{IP: net.ParseIP(lanIP), Port: lanPort}
		lanPreferred = true
	}

	e.peersMu.Lock()
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
		if lanIP != "" {
			p.LanIP = lanIP
		}
		if lanPort > 0 {
			p.LanPort = lanPort
		}
		if udpAddr != nil {
			p.UDPAddr = udpAddr
		}
	} else {
		e.peers[pid] = &PeerInfo{
			ClientID: pid, VirtualIP: vip, PubIP: pubIP,
			PubPort: pubPort, LanIP: lanIP, LanPort: lanPort,
			UDPAddr: udpAddr,
		}
	}
	e.peersMu.Unlock()

	if lanPreferred {
		log.Printf("[LAN] %s 与我同网段 (%s ↔ %s)，直连 %s:%d",
			pid, e.myLanIP, lanIP, lanIP, lanPort)
		e.relayMgr.MarkP2P(pid)
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
			ClientID: instr.TargetMac, VirtualIP: instr.TargetVirtualIp, UDPAddr: udpAddr,
		}
	}
}

func (e *Edge) runNatHole(instr *NatHoleInstruction) {
	res := e.executeNatHole(instr)

	if res == nil {
		return
	}

	if res.State == PunchStateSucceeded {
		e.relayMgr.MarkP2P(instr.TargetMac)
	} else if res.State == PunchStateFailed {
		e.relayMgr.MarkFallback(instr.TargetMac)
	}

	p2pStatus := 0
	switch e.relayMgr.GetState(instr.TargetMac) {
	case ConnP2P:
		p2pStatus = 3
	case ConnTURN, ConnRelay:
		p2pStatus = 2
	}

	_ = e.ws.Send(map[string]interface{}{
		"type": "p2p_state_info",
		"payload": map[string]interface{}{
			"to": []map[string]interface{}{{
				"macAddr": instr.TargetMac, "observedRaddr": "",
				"punchResult": res, "punchResultPeerMac": instr.TargetMac,
				"p2pStatus": p2pStatus,
			}},
		},
	})
}

// ============ IO ============

func (e *Edge) udpReadLoop() {
	buf := make([]byte, 65535)
	for {
		n, addr, err := e.udpConn.ReadFrom(buf)
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
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.notePeerProbe(ua)
			}
			continue
		}
		if buf[0]>>4 == 4 {
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.notePeerTraffic(ua)
			}
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

		e.peersMu.RLock()
		var target *PeerInfo
		for _, p := range e.peers {
			if p.VirtualIP == dstIP {
				target = p
				break
			}
		}
		e.peersMu.RUnlock()

		if target != nil {
			if !e.relayMgr.SendToPeer(target.ClientID, buf[:n], target) {
				_ = e.ws.SendBinary(buf[:n])
			}
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
	if dstIP != vip {
		return
	}
	e.enqueueTUN(data)
}

func (e *Edge) notePeerProbe(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false)
}

func (e *Edge) notePeerTraffic(addr *net.UDPAddr) {
	e.notePeerCommon(addr, true)
}

func (e *Edge) notePeerCommon(addr *net.UDPAddr, isRealData bool) {
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
	if isRealData {
		best.hasRealData = true
		if bestScore == 1 {
			best.UDPAddr = &net.UDPAddr{IP: addr.IP, Port: addr.Port}
		}
	}
	clientID := best.ClientID
	ip := addr.IP.String()
	realData := best.hasRealData
	e.peersMu.Unlock()

	if realData && isRealData && e.relayMgr.ShouldRelay(clientID) {
		log.Printf("[P2P] 从 %s (%s) 收到真实数据帧，升级为 P2P", clientID, ip)
		e.relayMgr.MarkP2P(clientID)
	}
}
