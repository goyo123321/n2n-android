package internal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

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

type PeerSnapshot struct {
	Code      string `json:"code"`
	ClientID  string `json:"clientId"`
	VirtualIP string `json:"vip"`
	Online    bool   `json:"online"`
	SharePort int    `json:"sharePort"`
	ConnType  string `json:"connType"`
	NATType   string `json:"natType"`
}

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

	udpConn net.PacketConn
	udpPort int

	peers   map[string]*PeerInfo
	peersMu sync.RWMutex

	tunWriteCh chan []byte
	natMeta    *NATMetadata

	netstack   *NetstackHost
	progress   *ProgressDispatcher
	wsOutbound *WSOutbound
	httpProxy  *HTTPProxy

	doneCh  chan struct{}
	closeMu sync.Mutex
	closed  bool
	mu      sync.Mutex // 保护 natMeta / ws / netstack / wsOutbound / httpProxy
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

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

	e := &Edge{
		cfg:        cfg,
		clientId:   clientId,
		nodeName:   nodeName,
		roomId:     cfg.RoomID,
		peers:      make(map[string]*PeerInfo),
		tunWriteCh: make(chan []byte, 4096),
		doneCh:     make(chan struct{}),
		natMeta: &NATMetadata{
			NATType:  "unknown",
			Behavior: "BehaviorPortChanged",
		},
	}

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
			// FilePacketConn 会 dup fd，无论成败都要关掉原 file
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

	e.progress = &ProgressDispatcher{}

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

		uuid := cfg.ConnectToken
		if uuid == "" {
			uuid = "2523c510-9ff0-415b-9582-93949bfae7e3"
		}

		wsOut, err := NewWSOutbound(
			cfg.SignalingURL, cfg.RoomID, clientId, cfg.ConnectToken,
			cfg.PreferredIP, cfg.PreferredPort,
			e.turnClient,
			uuid,
			e.GetVirtualIP(),
			e.udpPort,
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

	e.mu.Lock()
	wsOut := e.wsOutbound
	proxy := e.httpProxy
	ns := e.netstack
	e.wsOutbound = nil
	e.httpProxy = nil
	e.netstack = nil
	e.mu.Unlock()

	if wsOut != nil {
		wsOut.Close()
	}
	if proxy != nil {
		proxy.Close()
	}
	if ns != nil {
		_ = ns.Close()
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
		sharePort := item.p.SharePort
		if sharePort <= 0 {
			sharePort = 9090
		}
		snapshots = append(snapshots, PeerSnapshot{
			Code:      code,
			ClientID:  item.id,
			VirtualIP: item.p.VirtualIP,
			Online:    item.p.UDPAddr != nil || item.p.TurnRelayAddr != "",
			SharePort: sharePort,
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

		e.mu.Lock()
		nsExisting := e.netstack
		e.mu.Unlock()
		if nsExisting == nil && vip != "" {
			ns, err := NewNetstackHost(vip)
			if err != nil {
				log.Printf("[Netstack] 初始化失败: %v", err)
			} else {
				ns.progress = e.progress

				dnsCache := NewDNSCache()
				if dp, err := NewDNSProxy(dnsCache, func() *WSOutbound {
					e.mu.Lock()
					defer e.mu.Unlock()
					return e.wsOutbound
				}); err == nil {
					ns.SetDNSProxy(dp)
					log.Printf("[Netstack] DNS Proxy 已挂载（双 DNS）")
				}

				ns.SetProxyHandler(func(ip string, port int) (io.ReadWriteCloser, error) {
					e.mu.Lock()
					wsOut := e.wsOutbound
					e.mu.Unlock()
					if wsOut == nil {
						return nil, fmt.Errorf("wsOutbound 未就绪")
					}
					return wsOut.NewStream(ip, port)
				})
				log.Printf("[Netstack] ProxyHandler 已挂载")

				e.mu.Lock()
				if e.netstack != nil {
					e.mu.Unlock()
					_ = ns.Close()
				} else {
					e.netstack = ns
					e.mu.Unlock()

					go e.netstackReadLoop(ns)

					if e.cfg != nil && e.cfg.ShareDir != "" {
						if err := ns.StartShareServer(e.cfg.ShareDir); err != nil {
							log.Printf("[Netstack] 共享盘启动失败: %v", err)
						}
					}
				}
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
				log.Printf("[信令] 已有节点: %s vip=%s pub=%s:%d share=%d", pid, pip, pubIP, pubPort, sharePort)
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

	case "lan_direct":
		targetMac, _ := msg["targetMac"].(string)
		targetLanIP, _ := msg["targetLanIp"].(string)
		targetUdpPort := jsonInt(msg["targetUdpPort"])
		if targetMac == "" || targetLanIP == "" {
			return
		}
		log.Printf("[同 WiFi] 直连 %s @ %s:%d", targetMac, targetLanIP, targetUdpPort)

		e.peersMu.Lock()
		if p, ok := e.peers[targetMac]; ok {
			p.UDPAddr = &net.UDPAddr{IP: net.ParseIP(targetLanIP), Port: targetUdpPort}
			p.PubIP = targetLanIP
			p.PubPort = targetUdpPort
		} else {
			e.peers[targetMac] = &PeerInfo{
				ClientID: targetMac,
				UDPAddr:  &net.UDPAddr{IP: net.ParseIP(targetLanIP), Port: targetUdpPort},
			}
		}
		e.peersMu.Unlock()
		e.relayMgr.MarkP2P(targetMac)

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

func (e *Edge) reportMetadata() {
	e.mu.Lock()
	nm := e.natMeta
	ws := e.ws
	e.mu.Unlock()

	metaPayload := map[string]interface{}{
		"natType":            nm.NATType,
		"portsDifference":    nm.PortsDifference,
		"regularPortsChange": nm.RegularPortsChange,
		"behavior":           nm.Behavior,
		"assistedSockets":    nm.AssistedSockets,
		"sharePort":          9090,
		"p2pEndpoint":        nm.P2PEndpoint,
	}
	if nm.PublicEndpoint != "" {
		metaPayload["publicEndpoint"] = nm.PublicEndpoint
	}

	log.Printf("[信令] 上报 p2p_metadata: natType=%s publicEndpoint=%s p2pEndpoint=%s sharePort=9090",
		nm.NATType, nm.PublicEndpoint, nm.P2PEndpoint)

	if ws == nil {
		return
	}
	_ = ws.Send(map[string]interface{}{"type": "p2p_metadata", "payload": metaPayload})

	vip := e.GetVirtualIP()
	_ = ws.Send(map[string]interface{}{
		"type": "share_announce",
		"payload": map[string]interface{}{
			"name": e.nodeName, "virtualIp": vip, "port": 9090,
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
			ClientID: pid, VirtualIP: vip, PubIP: pubIP,
			PubPort: pubPort, SharePort: sharePort, UDPAddr: udpAddr,
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
			ClientID: instr.TargetMac, VirtualIP: instr.TargetVirtualIp, UDPAddr: udpAddr,
		}
	}
}

func (e *Edge) runNatHole(instr *NatHoleInstruction) {
	res := e.executeNatHole(instr)
	_ = e.ws.Send(map[string]interface{}{
		"type": "p2p_state_info",
		"payload": map[string]interface{}{
			"to": []map[string]interface{}{{
				"macAddr": instr.TargetMac, "observedRaddr": "",
				"punchResult": res, "punchResultPeerMac": instr.TargetMac,
			}},
		},
	})
	if res.State == PunchStateSucceeded {
		e.relayMgr.MarkP2P(instr.TargetMac)
	} else {
		e.relayMgr.MarkFallback(instr.TargetMac)
	}
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
				e.notePeerTraffic(ua)
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
		vip := e.GetVirtualIP()

		e.mu.Lock()
		ns := e.netstack
		e.mu.Unlock()

		if dstIP == vip && ns != nil {
			ns.InjectTUNPacket(buf[:n])
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

		if target != nil {
			if !e.relayMgr.SendToPeer(target.ClientID, buf[:n], target) {
				_ = e.ws.SendBinary(buf[:n])
			}
			continue
		}

		if ns != nil {
			ns.InjectTUNPacket(buf[:n])
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

// netstackReadLoop 阻塞等待 netstack 出包，通过 ctx 感知 Edge 停止。
//
// ctx 与 e.doneCh 关联：Edge.Stop 时 doneCh 关闭 → ctx 取消 →
// ReadTUNPacketContext 返回 nil → 循环退出。
//
// CPU 占用从原来 1ms 轮询的 ~5% 降到空闲时基本为 0。
func (e *Edge) netstackReadLoop(ns *NetstackHost) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 关联 e.doneCh：Edge 停止时取消 ctx
	go func() {
		select {
		case <-e.doneCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		data, ok := ns.ReadTUNPacketContext(ctx)
		if !ok {
			return
		}
		e.forwardPacketToPeer(data)
	}
}

func (e *Edge) forwardPacketToPeer(data []byte) {
	if len(data) < 20 || data[0]>>4 != 4 {
		return
	}
	dstIP := net.IP(data[16:20]).String()

	if dstIP == e.GetVirtualIP() {
		e.enqueueTUN(data)
		return
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

	if target != nil {
		if !e.relayMgr.SendToPeer(target.ClientID, data, target) {
			_ = e.ws.SendBinary(data)
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

	const sharePort = 9090
	proto := data[9]
	if proto == 6 && len(data) >= 24 {
		dstPort := int(binary.BigEndian.Uint16(data[22:24]))
		if dstPort == sharePort {
			e.mu.Lock()
			ns := e.netstack
			e.mu.Unlock()
			if ns != nil {
				ns.InjectTUNPacket(data)
			}
			return
		}
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
