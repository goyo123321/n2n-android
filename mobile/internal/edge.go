package internal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"
)

type PeerInfo struct {
	ClientID      string
	VirtualIP     string
	PubIP         string
	PubPort       int
	NATType       string
	TurnRelayAddr string
	UDPAddr       *net.UDPAddr
	lastRecvAt    int64
	loggedReady   bool
	hasRealData   bool
	probeReplied  bool
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

	serverSeenIP string

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

	fallbackTimers   map[string]*time.Timer
	fallbackTimersMu sync.Mutex

	lastNoPeerLog   map[string]int64
	lastNoPeerLogMu sync.Mutex

	tunStateLog   map[string]ConnType
	tunStateLogMu sync.Mutex

	doneCh  chan struct{}
	closeMu sync.Mutex
	closed  bool
	mu      sync.Mutex
}

func safeGo(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[panic] %s: %v\n%s", name, r, debug.Stack())
			}
		}()
		fn()
	}()
}

func isTransientReadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		return true
	}
	if errors.Is(err, syscall.EINTR) {
		return true
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "resource temporarily unavailable") ||
		strings.Contains(s, "interrupted system call") ||
		strings.Contains(s, "try again")
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

// ============ UDP 保活 ============

const keepaliveInterval = 5 * time.Second

var keepaliveServers = []string{
	"74.125.250.129:19302",
	"162.159.207.0:3478",
	"74.125.204.127:19302",
}

func buildSTUNBindingRequest() []byte {
	buf := make([]byte, 20)
	buf[0] = 0x00
	buf[1] = 0x01
	buf[2] = 0x00
	buf[3] = 0x00
	buf[4] = 0x21
	buf[5] = 0x12
	buf[6] = 0xA4
	buf[7] = 0x42
	if _, err := rand.Read(buf[8:20]); err != nil {
		binary.BigEndian.PutUint64(buf[8:16], uint64(time.Now().UnixNano()))
	}
	return buf
}

func (e *Edge) startKeepalive() {
	if e.udpConn == nil {
		return
	}

	var addrs []*net.UDPAddr
	for _, s := range keepaliveServers {
		if a, err := net.ResolveUDPAddr("udp4", s); err == nil {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		log.Printf("[Keepalive] ⚠️ 无可用 STUN 服务器，跳过保活")
		return
	}

	log.Printf("[Keepalive] 启动（条件触发），每 %v 刷新 %d 个 STUN 服务器（仅在有 peer 时）",
		keepaliveInterval, len(addrs))

	safeGo("keepalive", func() {
		sendOnce := func() {
			for _, addr := range addrs {
				probe := buildSTUNBindingRequest()
				_, _ = e.udpConn.WriteTo(probe, addr)
			}
		}

		hasPeers := func() bool {
			e.peersMu.RLock()
			defer e.peersMu.RUnlock()
			return len(e.peers) > 0
		}

		if hasPeers() {
			sendOnce()
		}

		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.doneCh:
				log.Printf("[Keepalive] 已停止")
				return
			case <-ticker.C:
				if !hasPeers() {
					continue
				}
				sendOnce()
			}
		}
	})
}

// ============ 日志节流 ============

func (e *Edge) logNoPeerThrottled(dstIP string, n int) {
	now := time.Now().UnixMilli()
	e.lastNoPeerLogMu.Lock()
	last := e.lastNoPeerLog[dstIP]
	if now-last < 5000 {
		e.lastNoPeerLogMu.Unlock()
		return
	}
	e.lastNoPeerLog[dstIP] = now
	e.lastNoPeerLogMu.Unlock()
	log.Printf("[TUN] 无匹配 peer，dst=%s len=%d", dstIP, n)
}

func (e *Edge) logTunForward(peerID, dstIP string, n, proto int, sent bool, state ConnType) {
	e.tunStateLogMu.Lock()
	prev, existed := e.tunStateLog[peerID]
	if existed && prev == state {
		e.tunStateLogMu.Unlock()
		return
	}
	e.tunStateLog[peerID] = state
	e.tunStateLogMu.Unlock()

	log.Printf("[TUN] → %s dst=%s len=%d proto=%d sent=%v state=%s",
		peerID, dstIP, n, proto, sent, state)
}

func (e *Edge) forgetTunState(peerID string) {
	e.tunStateLogMu.Lock()
	delete(e.tunStateLog, peerID)
	e.tunStateLogMu.Unlock()
}

// ============ 主流程 ============

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
	ws.SetHandlers(func(msg map[string]interface{}) {
		t, _ := msg["type"].(string)
		if t == "ready" {
			payload, _ := msg["payload"].(map[string]interface{})
			vip, _ := payload["virtualIp"].(string)
			select {
			case done <- vip:
			default:
			}
		}
	}, nil)

	select {
	case vip := <-done:
		log.Printf("[FetchVIP] 拿到虚拟 IP: %s", vip)
		return vip
	case <-time.After(15 * time.Second):
		log.Printf("[FetchVIP] 超时")
		return ""
	}
}

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

	e := &Edge{
		cfg:            cfg,
		clientId:       clientId,
		nodeName:       nodeName,
		roomId:         cfg.RoomID,
		peers:          make(map[string]*PeerInfo),
		fallbackTimers: make(map[string]*time.Timer),
		lastNoPeerLog:  make(map[string]int64),
		tunStateLog:    make(map[string]ConnType),
		tunWriteCh:     make(chan []byte, 4096),
		doneCh:         make(chan struct{}),
		natMeta: &NATMetadata{
			NATType:  "unknown",
			Behavior: "BehaviorPortChanged",
		},
	}

	tun, err := setupTUNFromFD(tunFd)
	if err != nil {
		return nil, fmt.Errorf("包装 TUN fd 失败: %w", err)
	}
	e.tun = tun

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

	e.turnClient = NewTURNClient(cfg.SignalingURL, cfg.ConnectToken, e)
	e.turnClient.onMessage = func(data []byte, addr net.Addr) {
		ua, ok := addr.(*net.UDPAddr)
		if !ok {
			e.onRemotePacket(data)
			return
		}
		// probe 判定（首 4 字节 "N2NP"）
		if len(data) >= 4 &&
			data[0] == 'N' && data[1] == '2' && data[2] == 'N' && data[3] == 'P' {
			e.notePeerProbeTURN(ua)
			return
		}
		// 真实 IPv4 数据帧
		if len(data) > 0 && data[0]>>4 == 4 {
			e.notePeerTrafficTURN(ua)
			e.onRemotePacket(data)
			return
		}
	}

	e.relayMgr = NewRelayManager(ws, e.turnClient, e)
	e.relayMgr.Report(10 * time.Second)

	ws.SetHandlers(
		e.handleSignaling,
		func(data []byte) {
			e.onRemotePacket(data)
		},
	)

	safeGo("udpReadLoop", e.udpReadLoop)
	safeGo("tunWriteLoop", e.tunWriteLoop)
	safeGo("tunReadLoop", e.tunReadLoop)

	e.startKeepalive()

	safeGo("nat-probe", func() {
		log.Printf("[NAT] 开始异步 STUN 探测...")
		var meta *NATMetadata
		if stunConn != nil {
			meta = probeNATWithConn(stunConn, nil)
			_ = stunConn.Close()
		} else {
			meta = probeNAT(e.udpPort, nil)
		}
		if meta == nil {
			meta = &NATMetadata{NATType: "unknown", Behavior: "BehaviorPortChanged"}
		}

		e.mu.Lock()
		serverIP := e.serverSeenIP
		e.mu.Unlock()

		if serverIP != "" && meta.PublicEndpoint != "" {
			stunIP := extractIPFromEndpoint(meta.PublicEndpoint)
			if stunIP != "" && stunIP != serverIP {
				log.Printf(
					"[NAT] ⚠️ WS/STUN 出口不一致：WS=%s STUN=%s —— CGNAT 池化，打洞大概率失败",
					serverIP, stunIP,
				)
				meta.MultiExit = true
				meta.NATType = "HardNAT"
				meta.Behavior = "BehaviorPortChanged"
			} else if stunIP != "" {
				log.Printf("[NAT] ✅ WS/STUN 出口一致：%s —— 出口稳定", stunIP)
			}
		}

		e.mu.Lock()
		e.natMeta = meta
		wsRef := e.ws
		e.mu.Unlock()

		log.Printf("[NAT] 探测完成: %s pub=%s multiExit=%v",
			meta.NATType, meta.PublicEndpoint, meta.MultiExit)

		if wsRef != nil {
			e.reportMetadata()
			log.Printf("[NAT] 已重新上报 p2p_metadata")
		}
	})

	safeGo("turn-init", func() {
		time.Sleep(200 * time.Millisecond)
		ctx, cancel := contextWithTimeout(30 * time.Second)
		defer cancel()

		if e.turnClient == nil {
			return
		}
		if err := e.turnClient.FetchAndSetup(ctx); err != nil {
			log.Printf("[TURN] 初始化失败: %v", err)
			return
		}
		log.Printf("[TURN] 就绪: %s", e.turnClient.GetRelayAddr())
		if e.relayMgr != nil {
			e.relayMgr.UpgradeRelaysToTURN()
		}
		if e.ws != nil {
			_ = e.ws.Send(map[string]interface{}{
				"type":      "turn_relay_info",
				"relayAddr": e.turnClient.GetRelayAddr(),
			})
		}
	})

	e.turnClient.StartReconnectLoop()

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

	e.fallbackTimersMu.Lock()
	for _, t := range e.fallbackTimers {
		t.Stop()
	}
	e.fallbackTimers = make(map[string]*time.Timer)
	e.fallbackTimersMu.Unlock()

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
		id      string
		vip     string
		online  bool
		natType string
	}
	var list []pair
	for id, p := range e.peers {
		list = append(list, pair{
			id:      id,
			vip:     p.VirtualIP,
			online:  p.UDPAddr != nil || p.TurnRelayAddr != "",
			natType: p.NATType,
		})
	}
	e.peersMu.RUnlock()

	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[i].id > list[j].id {
				list[i], list[j] = list[j], list[i]
			}
		}
	}

	snapshots := make([]PeerSnapshot, 0, len(list))
	for i, item := range list {
		connType := "unknown"
		if e.relayMgr != nil {
			connType = string(e.relayMgr.GetState(item.id))
		}
		natType := item.natType
		if natType == "" {
			natType = "unknown"
		}
		snapshots = append(snapshots, PeerSnapshot{
			Code:      idxToCode(i),
			ClientID:  item.id,
			VirtualIP: item.vip,
			Online:    item.online,
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

func extractIPFromEndpoint(ep string) string {
	if ep == "" {
		return ""
	}
	i := lastIndexByte(ep, ':')
	if i < 0 {
		return ep
	}
	return ep[:i]
}

func lastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// ============ 信令 ============

func (e *Edge) handleSignaling(msg map[string]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[信令] handleSignaling panic: %v\n%s", r, debug.Stack())
		}
	}()

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

		if serverIP, ok := payload["yourPublicIp"].(string); ok && serverIP != "" {
			e.mu.Lock()
			e.serverSeenIP = serverIP
			e.mu.Unlock()
			log.Printf("[信令] 服务端看到的本机出口 IP: %s（WS/TCP 出口，仅参考）", serverIP)
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
				relayAddr, _ := pm["turnRelayAddr"].(string)
				natType, _ := pm["natType"].(string)
				if pid == "" || pip == "" {
					continue
				}
				e.registerPeer(pid, pip, pubIP, pubPort, natType)
				if relayAddr != "" {
					e.peersMu.Lock()
					if pi, ok := e.peers[pid]; ok {
						pi.TurnRelayAddr = relayAddr
					}
					e.peersMu.Unlock()
				}
				log.Printf("[信令] 已有节点: %s vip=%s pub=%s:%d nat=%s",
					pid, pip, pubIP, pubPort, natType)
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
		natType, _ := payload["natType"].(string)

		log.Printf("[信令] joined: from=%s vip=%s pub=%s:%d nat=%s",
			from, pip, pubIP, pubPort, natType)

		if from != "" && pip != "" {
			e.registerPeer(from, pip, pubIP, pubPort, natType)
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
		if !e.peerExists(instr.TargetMac) {
			log.Printf("[NAT-HOLE] 忽略指令 target=%s（peer 不在线）", instr.TargetMac)
			return
		}
		e.ensureTargetPeer(&instr)
		e.scheduleFallbackTimer(instr.TargetMac)
		instrCopy := instr
		safeGo("nat-hole", func() { e.runNatHole(&instrCopy) })

	case "force_fallback":
		payload, _ := msg["payload"].(map[string]interface{})
		if payload == nil {
			return
		}
		peersRaw, _ := payload["peers"].([]interface{})
		reason, _ := payload["reason"].(string)
		if reason == "" {
			reason = "server-forced"
		}
		log.Printf("[信令] 收到 force_fallback: %d 个 peer, reason=%s", len(peersRaw), reason)
		for _, pRaw := range peersRaw {
			peerID, _ := pRaw.(string)
			if peerID == "" {
				continue
			}
			if e.relayMgr == nil {
				continue
			}
			if e.relayMgr.GetState(peerID) == ConnP2P {
				continue
			}
			e.relayMgr.MarkFallback(peerID)
		}
		return

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
		e.cancelFallbackTimer(from)
		e.lastNoPeerLogMu.Lock()
		delete(e.lastNoPeerLog, from)
		e.lastNoPeerLogMu.Unlock()
		e.forgetTunState(from)

	case "connection_status":
		return
	}
}

func (e *Edge) peerExists(peerID string) bool {
	if peerID == "" {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	_, ok := e.peers[peerID]
	return ok
}

func (e *Edge) reportMetadata() {
	e.mu.Lock()
	nm := e.natMeta
	ws := e.ws
	serverSeenIP := e.serverSeenIP
	e.mu.Unlock()

	if nm == nil {
		nm = &NATMetadata{NATType: "unknown", Behavior: "BehaviorPortChanged"}
	}

	if serverSeenIP != "" && nm.PublicEndpoint != "" {
		stunIP := extractIPFromEndpoint(nm.PublicEndpoint)
		if stunIP != "" && stunIP != serverSeenIP {
			nm.MultiExit = true
			nm.NATType = "HardNAT"
			nm.Behavior = "BehaviorPortChanged"
		}
	}

	metaPayload := map[string]interface{}{
		"name":               e.nodeName,
		"natType":            nm.NATType,
		"portsDifference":    nm.PortsDifference,
		"regularPortsChange": nm.RegularPortsChange,
		"behavior":           nm.Behavior,
		"p2pEndpoint":        nm.P2PEndpoint,
		"multiExit":          nm.MultiExit,
	}
	if nm.PublicEndpoint != "" {
		metaPayload["publicEndpoint"] = nm.PublicEndpoint
	}

	log.Printf("[信令] 上报 p2p_metadata: natType=%s publicEndpoint=%q multiExit=%v",
		nm.NATType, nm.PublicEndpoint, nm.MultiExit)

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

func (e *Edge) registerPeer(pid, vip, pubIP string, pubPort int, natType string) {
	var udpAddr *net.UDPAddr
	if pubIP != "" && pubPort > 0 {
		udpAddr = &net.UDPAddr{IP: net.ParseIP(pubIP), Port: pubPort}
	}

	e.peersMu.Lock()
	_, existed := e.peers[pid]
	if existed {
		p := e.peers[pid]
		if vip != "" {
			p.VirtualIP = vip
		}
		if pubIP != "" {
			p.PubIP = pubIP
		}
		if pubPort > 0 {
			p.PubPort = pubPort
		}
		if udpAddr != nil {
			p.UDPAddr = udpAddr
		}
		if natType != "" {
			p.NATType = natType
		}
	} else {
		e.peers[pid] = &PeerInfo{
			ClientID: pid, VirtualIP: vip, PubIP: pubIP,
			PubPort: pubPort, UDPAddr: udpAddr,
			NATType: natType,
		}
	}
	e.peersMu.Unlock()

	if !existed {
		e.scheduleFallbackTimer(pid)
	}
}

func (e *Edge) scheduleFallbackTimer(peerID string) {
	if e.relayMgr == nil || peerID == "" {
		return
	}

	e.fallbackTimersMu.Lock()
	if t, ok := e.fallbackTimers[peerID]; ok {
		t.Stop()
	}
	t := time.AfterFunc(8*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[fallback-timer] panic: %v", r)
			}
		}()

		e.fallbackTimersMu.Lock()
		delete(e.fallbackTimers, peerID)
		e.fallbackTimersMu.Unlock()

		if e.relayMgr == nil {
			return
		}
		state := e.relayMgr.GetState(peerID)
		if state != ConnUnknown {
			if state != ConnP2P {
				log.Printf("[fallback-timer] %s 已有状态 %s，跳过降级", peerID, state)
			}
			return
		}
		log.Printf("[fallback-timer] %s 8s 内未收到打洞指令，主动降级", peerID)
		e.relayMgr.MarkFallback(peerID)
	})
	e.fallbackTimers[peerID] = t
	e.fallbackTimersMu.Unlock()
}

func (e *Edge) cancelFallbackTimer(peerID string) {
	e.fallbackTimersMu.Lock()
	defer e.fallbackTimersMu.Unlock()
	if t, ok := e.fallbackTimers[peerID]; ok {
		t.Stop()
		delete(e.fallbackTimers, peerID)
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

	if e.ws == nil {
		return
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
		select {
		case <-e.doneCh:
			return
		default:
		}

		n, addr, err := e.udpConn.ReadFrom(buf)
		if err != nil {
			select {
			case <-e.doneCh:
				return
			default:
			}
			if isTransientReadError(err) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			log.Printf("[P2P] UDP 读错误，退出读循环: %v", err)
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
		select {
		case <-e.doneCh:
			return
		default:
		}

		n, err := e.tun.Read(buf)
		if err != nil {
			select {
			case <-e.doneCh:
				return
			default:
			}
			if isTransientReadError(err) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			log.Printf("[TUN] 读错误，退出读循环: %v", err)
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
			state := ConnUnknown
			if e.relayMgr != nil {
				state = e.relayMgr.GetState(target.ClientID)
			}
			sent := false
			if e.relayMgr != nil {
				sent = e.relayMgr.SendToPeer(target.ClientID, buf[:n], target)
			}
			if n >= 10 {
				e.logTunForward(target.ClientID, dstIP, n, int(buf[9]), sent, state)
			}
			if !sent && e.ws != nil {
				_ = e.ws.SendBinary(buf[:n])
			}
		} else {
			e.logNoPeerThrottled(dstIP, n)
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

// ============ peer 探测处理 ============

func (e *Edge) notePeerProbe(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false, false)
}

func (e *Edge) notePeerProbeTURN(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false, true)
}

func (e *Edge) notePeerTraffic(addr *net.UDPAddr) {
	e.notePeerCommon(addr, true, false)
}

func (e *Edge) notePeerTrafficTURN(addr *net.UDPAddr) {
	e.notePeerCommon(addr, true, true)
}

// notePeerCommon 处理来自对端的 UDP 包。
//
// viaTURN：
//   - false：从本地 UDP socket 收到 → 回包走本地 socket
//   - true：从 TURN 中继收到 → 回包走 TURN（按原路）
//
// 首次收到 probe 的处理顺序：
//   1. 立即上报 P2P（MarkP2P）
//   2. 并发启动 5 轮 probe 回包（每 100ms 一次）
//
// 后续收到的 probe 只刷新 lastRecvAt，不再重复回包。
func (e *Edge) notePeerCommon(addr *net.UDPAddr, isRealData bool, viaTURN bool) {
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
	}
	best.UDPAddr = &net.UDPAddr{IP: addr.IP, Port: addr.Port}
	clientID := best.ClientID
	ip := addr.IP.String()

	needReply := false
	if !isRealData && !best.probeReplied {
		best.probeReplied = true
		needReply = true
	}
	e.peersMu.Unlock()

	if e.relayMgr == nil {
		return
	}

	if isRealData {
		state := e.relayMgr.GetState(clientID)
		if state != ConnP2P {
			log.Printf("[P2P] 从 %s (%s) 收到真实数据帧，升级为 P2P", clientID, ip)
			e.relayMgr.MarkP2P(clientID)
		}
		return
	}

	if !needReply {
		return
	}

	// ★ 首次收到该 peer 的 probe：
	//    1. 立即上报 P2P
	//    2. 并发回 5 轮（每 100ms 一次）
	if e.relayMgr.GetState(clientID) != ConnP2P {
		log.Printf("[P2P] 从 %s (%s) 收到打洞探测，上报 P2P 并回 5 轮", clientID, ip)
		e.relayMgr.MarkP2P(clientID)
	}

	safeGo("probe-reply-5", func() {
		probe := buildPunchProbe(e.GetVirtualIP())
		for i := 0; i < 5; i++ {
			select {
			case <-e.doneCh:
				return
			default:
			}
			e.writeProbe(addr, probe, viaTURN)
			if i < 4 {
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
}

// writeProbe 按原路写一个 probe 包。
func (e *Edge) writeProbe(addr *net.UDPAddr, probe []byte, viaTURN bool) {
	if addr == nil {
		return
	}
	if viaTURN {
		if e.turnClient == nil || !e.turnClient.IsReady() {
			return
		}
		_ = e.turnClient.Send(probe, addr)
		return
	}
	if e.udpConn == nil {
		return
	}
	_, _ = e.udpConn.WriteTo(probe, addr)
}

// sendProbeTo 保留兼容（当前无调用方）。
func (e *Edge) sendProbeTo(addr *net.UDPAddr) {
	if e.udpConn == nil || addr == nil {
		return
	}
	probe := buildPunchProbe(e.GetVirtualIP())
	e.writeProbe(addr, probe, false)
}

func (e *Edge) hasTrafficFromTarget(peerID string, since int64) bool {
	if peerID == "" {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	p := e.peers[peerID]
	if p == nil || p.UDPAddr == nil {
		return false
	}
	return p.lastRecvAt >= since
}

// hasTrafficFromAny 保留兼容（当前已无调用方）。
func (e *Edge) hasTrafficFromAny(ips []net.IP, since int64) bool {
	if len(ips) == 0 {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	for _, p := range e.peers {
		if p.UDPAddr == nil || p.lastRecvAt < since {
			continue
		}
		for _, ip := range ips {
			if p.UDPAddr.IP.Equal(ip) {
				return true
			}
		}
	}
	return false
}
