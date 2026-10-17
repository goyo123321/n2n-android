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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type PeerInfo struct {
	ClientID          string
	VirtualIP         string
	PubIP             string
	PubPort           int
	NATType           string
	TurnRelayAddr     string
	UDPAddr           *net.UDPAddr
	lastRecvAt        int64
	lastLANRecvAt     int64
	loggedReady       bool
	hasRealData       bool
	probeReplied      bool
	AssistedEndpoints []string
	LanEndpoints      []string
}

// ★ 补上漏掉的结构体
type PeerSnapshot struct {
	Code      string `json:"code"`
	ClientID  string `json:"clientId"`
	VirtualIP string `json:"vip"`
	Online    bool   `json:"online"`
	ConnType  string `json:"connType"`
	NATType   string `json:"natType"`
}

type Edge struct {
	cfg         *Config
	clientId    string
	nodeName    string
	virtualIP   string
	virtualCIDR string
	roomId      string

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

	p2pAvailTimers   map[string]*time.Timer
	p2pAvailTimersMu sync.Mutex

	p2pStreams *P2PStreamManager
	streamMgr  *AppStreamManager
	socks5Srv  *Socks5Server

	punchAckLimiter *punchAckLimiter

	lastNoPeerLog   map[string]int64
	lastNoPeerLogMu sync.Mutex

	tunStateLog   map[string]ConnType
	tunStateLogMu sync.Mutex

	lastMetadataReportAt int64

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

const keepaliveInterval = 5 * time.Second

var keepaliveServers = []string{
	"74.125.250.129:19302",
	"74.125.204.127:19302",
	"162.159.207.0:3478",
	"stun.miwifi.com:3478",
	"stun.cloudflare.com:3478",
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

	log.Printf("[Keepalive] 启动，每 %v 刷新 %d 个 STUN 服务器（无条件，保 CGNAT 映射）",
		keepaliveInterval, len(addrs))

	safeGo("keepalive", func() {
		sendOnce := func() {
			for _, addr := range addrs {
				probe := buildSTUNBindingRequest()
				_, _ = e.udpConn.WriteTo(probe, addr)
			}
		}

		sendOnce()

		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.doneCh:
				log.Printf("[Keepalive] 已停止")
				return
			case <-ticker.C:
				sendOnce()
			}
		}
	})
}

const p2pKeepaliveInterval = 15 * time.Second
const p2pIdleDeadMs = 120000

func (e *Edge) startP2PKeepalive() {
	log.Printf("[P2P-Keepalive] 启动，每 %v 刷新 P2P 通道", p2pKeepaliveInterval)

	safeGo("p2p-keepalive", func() {
		ticker := time.NewTicker(p2pKeepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.doneCh:
				log.Printf("[P2P-Keepalive] 已停止")
				return
			case <-ticker.C:
				e.sendP2PKeepalive()
			}
		}
	})
}

func (e *Edge) sendP2PKeepalive() {
	if e.relayMgr == nil || e.p2pStreams == nil {
		return
	}

	now := time.Now().UnixMilli()

	e.peersMu.RLock()
	var deadPeers []string
	var alivePeers []string
	for id, p := range e.peers {
		if p.UDPAddr == nil {
			continue
		}
		st := e.relayMgr.GetState(id)

		if st == ConnP2P || st == ConnP2PAvailable {
			idleMs := now - p.lastRecvAt
			if p.lastRecvAt > 0 && idleMs > p2pIdleDeadMs {
				deadPeers = append(deadPeers, id)
				continue
			}
		}

		alivePeers = append(alivePeers, id)
	}
	e.peersMu.RUnlock()

	for _, id := range deadPeers {
		log.Printf("[P2P] %s %ds 无任何流量，判定 P2P 失效，强制降级",
			id, p2pIdleDeadMs/1000)
		e.relayMgr.ForceFallback(id, "no-traffic")
	}

	for _, id := range alivePeers {
		e.p2pStreams.sendP2PProbe(id)
	}
}

func (e *Edge) getVirtualIPLocked() string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	return e.virtualIP
}

func (e *Edge) logNoPeerThrottled(dstIP string, n int) {
	now := time.Now().UnixMilli()
	e.lastNoPeerLogMu.Lock()
	last := e.lastNoPeerLog["__global__"]
	if now-last < 5000 {
		e.lastNoPeerLogMu.Unlock()
		return
	}
	e.lastNoPeerLog["__global__"] = now
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

	type result struct {
		vip  string
		cidr string
	}
	done := make(chan result, 1)

	ws.SetHandlers(func(msg map[string]interface{}) {
		t, _ := msg["type"].(string)
		if t == "ready" {
			payload, _ := msg["payload"].(map[string]interface{})
			vip, _ := payload["virtualIp"].(string)
			cidr, _ := payload["virtualNetwork"].(string)
			if cidr == "" {
				cidr = "10.64.0.0/24"
			}
			select {
			case done <- result{vip: vip, cidr: cidr}:
			default:
			}
		}
	}, nil)

	select {
	case r := <-done:
		log.Printf("[FetchVIP] 拿到虚拟 IP: %s（网段 %s）", r.vip, r.cidr)
		return r.vip + "|" + r.cidr
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
		virtualCIDR:    "10.64.0.0/24",
		peers:          make(map[string]*PeerInfo),
		fallbackTimers: make(map[string]*time.Timer),
		p2pAvailTimers: make(map[string]*time.Timer),
		lastNoPeerLog:  make(map[string]int64),
		tunStateLog:    make(map[string]ConnType),
		tunWriteCh:     make(chan []byte, 4096),
		doneCh:         make(chan struct{}),
		natMeta: &NATMetadata{
			NATType:  "unknown",
			Behavior: "BehaviorPortChanged",
		},
	}
	e.punchAckLimiter = newPunchAckLimiter()
	e.streamMgr = NewAppStreamManager()

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
		if isP2PFrame(data) {
			e.handleP2PFrame(ua, true, data)
			return
		}
		if isPunchProbe(data) {
			e.notePeerProbeTURN(ua)
			return
		}
		if isPunchAck(data) {
			e.notePeerAckTURN(ua)
			return
		}
		if len(data) > 0 && data[0]>>4 == 4 {
			e.notePeerTrafficTURN(ua)
			e.onRemotePacket(data)
			return
		}
	}

	e.relayMgr = NewRelayManager(ws, e.turnClient, e)
	e.relayMgr.Report(10 * time.Second)

	e.p2pStreams = NewP2PStreamManager(e)

	ws.SetHandlers(
		e.handleSignaling,
		func(data []byte) {
			if len(data) >= 4 && string(data[:4]) == "N2NW" {
				e.handleAppStreamBinary(data)
				return
			}
			e.onRemotePacket(data)
		},
	)

	safeGo("udpReadLoop", e.udpReadLoop)
	safeGo("tunWriteLoop", e.tunWriteLoop)
	safeGo("tunReadLoop", e.tunReadLoop)

	e.startKeepalive()
	e.startP2PKeepalive()

	if cfg.Socks5Listen != "" {
		e.socks5Srv = NewSocks5Server(e, cfg.Socks5Listen)
		safeGo("socks5-server", func() { e.socks5Srv.Serve() })
		log.Printf("[SOCKS5] 已启用，监听 %s", cfg.Socks5Listen)
	}

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

		log.Printf("[NAT] 探测完成: %s pub=%s multiExit=%v endpoints=%d",
			meta.NATType, meta.PublicEndpoint, meta.MultiExit, len(meta.AllEndpoints))

		if wsRef != nil {
			e.reportMetadata()
			atomic.StoreInt64(&e.lastMetadataReportAt, time.Now().UnixMilli())
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
	e.startFailCountResetLoop()

	log.Printf("[Edge] 已启动 clientId=%s room=%s cidr=%s", clientId, cfg.RoomID, e.virtualCIDR)
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

	e.p2pAvailTimersMu.Lock()
	for _, t := range e.p2pAvailTimers {
		t.Stop()
	}
	e.p2pAvailTimers = make(map[string]*time.Timer)
	e.p2pAvailTimersMu.Unlock()

	if e.ws != nil {
		_ = e.ws.Close()
	}
	if e.turnClient != nil {
		e.turnClient.Close()
	}
	if e.udpConn != nil {
		_ = e.udpConn.Close()
	}
	if e.socks5Srv != nil {
		e.socks5Srv.Close()
	}
	if e.streamMgr != nil {
		e.streamMgr.closeAll()
	}
	if e.p2pStreams != nil {
		e.p2pStreams.closeAll()
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

func (e *Edge) GetVirtualNetwork() string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	return e.virtualCIDR
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

func (e *Edge) scheduleP2PAvailTimeout(peerID string) {
	if e.relayMgr == nil || peerID == "" {
		return
	}

	e.p2pAvailTimersMu.Lock()
	if t, ok := e.p2pAvailTimers[peerID]; ok {
		t.Stop()
	}

	t := time.AfterFunc(30*time.Second, func() {
		e.p2pAvailTimersMu.Lock()
		delete(e.p2pAvailTimers, peerID)
		e.p2pAvailTimersMu.Unlock()

		if e.relayMgr == nil {
			return
		}
		if e.relayMgr.GetState(peerID) == ConnP2PAvailable {
			log.Printf("[P2P] %s P2PAvailable 超时 30s 未验证数据面，强制降级", peerID)
			e.relayMgr.ForceFallback(peerID, "p2p-avail-timeout")
		}
	})
	e.p2pAvailTimers[peerID] = t
	e.p2pAvailTimersMu.Unlock()
}

func (e *Edge) cancelP2PAvailTimeout(peerID string) {
	if peerID == "" {
		return
	}
	e.p2pAvailTimersMu.Lock()
	defer e.p2pAvailTimersMu.Unlock()
	if t, ok := e.p2pAvailTimers[peerID]; ok {
		t.Stop()
		delete(e.p2pAvailTimers, peerID)
	}
}

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

		if e.relayMgr != nil {
			e.relayMgr.ClearAll()
		}
		if e.p2pStreams != nil {
			e.p2pStreams.closeAll()
		}
		if e.streamMgr != nil {
			e.streamMgr.closeAll()
		}
		if e.punchAckLimiter != nil {
			e.punchAckLimiter.clear()
		}

		e.p2pAvailTimersMu.Lock()
		for _, tt := range e.p2pAvailTimers {
			tt.Stop()
		}
		e.p2pAvailTimers = make(map[string]*time.Timer)
		e.p2pAvailTimersMu.Unlock()

		natHoleActiveMu.Lock()
		failCount := len(failCounts)
		if failCount > 0 {
			failCounts = make(map[string]int)
		}
		natHoleActiveMu.Unlock()
		if failCount > 0 {
			log.Printf("[NAT-HOLE] WS 重连，清空 %d 个 peer 的失败计数", failCount)
		}

		e.reportMetadata()
		atomic.StoreInt64(&e.lastMetadataReportAt, time.Now().UnixMilli())
		return
	}

	switch t {
	case "ready":
		payload, _ := msg["payload"].(map[string]interface{})
		e.peersMu.Lock()
		e.virtualIP, _ = payload["virtualIp"].(string)
		if cidr, ok := payload["virtualNetwork"].(string); ok && cidr != "" {
			e.virtualCIDR = cidr
		}
		vip := e.virtualIP
		cidr := e.virtualCIDR
		e.peersMu.Unlock()
		log.Printf("[信令] 分配虚拟 IP: %s（网段 %s）", vip, cidr)

		if serverIP, ok := payload["yourPublicIp"].(string); ok && serverIP != "" {
			e.mu.Lock()
			e.serverSeenIP = serverIP
			e.mu.Unlock()
			log.Printf("[信令] 服务端看到的本机出口 IP: %s（WS/TCP 出口，仅参考）", serverIP)
		}

		e.reportMetadata()
		atomic.StoreInt64(&e.lastMetadataReportAt, time.Now().UnixMilli())

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

				var assisted []string
				if raw, ok := pm["assistedEndpoints"].([]interface{}); ok {
					for _, v := range raw {
						if s, ok := v.(string); ok && s != "" {
							assisted = append(assisted, s)
						}
					}
				}
				var lanEndpoints []string
				if raw, ok := pm["lanEndpoints"].([]interface{}); ok {
					for _, v := range raw {
						if s, ok := v.(string); ok && s != "" {
							lanEndpoints = append(lanEndpoints, s)
						}
					}
				}

				if pid == "" || pip == "" {
					continue
				}

				e.maybeResetFailCountForPeer(pid, pubIP, pubPort)

				e.registerPeer(pid, pip, pubIP, pubPort, natType, assisted, lanEndpoints)
				if relayAddr != "" {
					e.peersMu.Lock()
					if pi, ok := e.peers[pid]; ok {
						pi.TurnRelayAddr = relayAddr
					}
					e.peersMu.Unlock()
				}
				log.Printf("[信令] 已有节点: %s vip=%s pub=%s:%d nat=%s assisted=%d lan=%d",
					pid, pip, pubIP, pubPort, natType, len(assisted), len(lanEndpoints))
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

		var assisted []string
		if raw, ok := payload["assistedEndpoints"].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok && s != "" {
					assisted = append(assisted, s)
				}
			}
		}
		var lanEndpoints []string
		if raw, ok := payload["lanEndpoints"].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok && s != "" {
					lanEndpoints = append(lanEndpoints, s)
				}
			}
		}

		log.Printf("[信令] joined: from=%s vip=%s pub=%s:%d nat=%s assisted=%d lan=%d",
			from, pip, pubIP, pubPort, natType, len(assisted), len(lanEndpoints))

		if from != "" && pip != "" {
			e.maybeResetFailCountForPeer(from, pubIP, pubPort)

			e.registerPeer(from, pip, pubIP, pubPort, natType, assisted, lanEndpoints)
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
			e.relayMgr.ForceFallback(peerID, reason)
		}

	case "turn_peer_info":
		edgeMac, _ := msg["edgeMac"].(string)
		relayAddr, _ := msg["relayAddr"].(string)
		if edgeMac != "" && relayAddr != "" {
			e.peersMu.Lock()
			if p, ok := e.peers[edgeMac]; ok {
				p.TurnRelayAddr = relayAddr
			}
			e.peersMu.Unlock()
			log.Printf("[TURN] 记录 %s 的中继地址: %s", edgeMac, relayAddr)
		}

	case "app_stream_open":
		safeGo("app-stream-open", func() { e.onRemoteStreamOpen(msg) })
	case "app_stream_open_ack":
		e.onRemoteStreamAck(msg)
	case "app_stream_data":
		e.onRemoteStreamData(msg)
	case "app_stream_close":
		e.onRemoteStreamClose(msg)

	case "pong":
		return

	case "left":
		log.Printf("[信令] 节点离开: %s", from)

		if e.relayMgr != nil {
			e.relayMgr.ClearPeer(from)
		}
		if e.punchAckLimiter != nil {
			e.punchAckLimiter.forget(from)
		}
		e.cancelP2PAvailTimeout(from)

		natHoleActiveMu.Lock()
		delete(failCounts, from)
		delete(natHoleActive, from)
		natHoleActiveMu.Unlock()

		e.peersMu.Lock()
		delete(e.peers, from)
		e.peersMu.Unlock()

		e.cancelFallbackTimer(from)
		e.lastNoPeerLogMu.Lock()
		delete(e.lastNoPeerLog, from)
		e.lastNoPeerLogMu.Unlock()
		e.forgetTunState(from)
		if e.p2pStreams != nil {
			e.p2pStreams.dropPeer(from)
		}

	case "connection_status":
		return
	}
}
// 保持文件其余部分（maybeResetFailCountForPeer 等）不变
