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
	loggedReady       bool
	hasRealData       bool
	AssistedEndpoints []string
	LanEndpoints      []string // ★ 新增
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

	// ★ 新增：P2PAvailable 超时降级定时器
	p2pAvailTimers   map[string]*time.Timer
	p2pAvailTimersMu sync.Mutex

	// ★ 新增：N2NS P2P 流协议管理器
	p2pStreams *P2PStreamManager

	// ★ 新增：Punch ACK 限流器
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

// ============ UDP 保活 ============

const keepaliveInterval = 5 * time.Second

var keepaliveServers = []string{
	"74.125.250.129:19302",
	"162.159.207.0:3478",
	"stun.miwifi.com:3478",
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

// ============ P2P 通道保活（★ 新增） ============

const p2pKeepaliveInterval = 15 * time.Second

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

// sendP2PKeepalive 用 N2NS PING 多目标发送，替代单目标 Punch。
//
// 走 p2pStreams.sendP2PProbe → sendP2PFrame → collectN2NSTargets
// 多目标发送（LAN + assisted + lastRecvFrom + peerAddr + peerUDPAddr），
// 大幅提升 Keepalive 命中率。
func (e *Edge) sendP2PKeepalive() {
	if e.relayMgr == nil || e.p2pStreams == nil {
		return
	}

	e.peersMu.RLock()
	var targets []string
	for id, p := range e.peers {
		if p.UDPAddr == nil {
			continue
		}
		st := e.relayMgr.GetState(id)
		if st != ConnP2P && st != ConnP2PAvailable {
			continue
		}
		targets = append(targets, id)
	}
	e.peersMu.RUnlock()

	if len(targets) == 0 {
		return
	}

	for _, id := range targets {
		e.p2pStreams.sendP2PProbe(id)
	}
}

func (e *Edge) getVirtualIPLocked() string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	return e.virtualIP
}

// ============ 日志节流 ============

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

// ============ 主流程 ============

// FetchVirtualIP 返回 "vip|cidr" 格式，例如 "172.16.0.2|172.16.0.0/20"
//
// Kotlin 侧按 '|' split 拿 vip 和 cidr。
// 失败返回空字符串。
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
		// N2NS 数据面（★ 新增）
		if isP2PFrame(data) {
			e.handleP2PFrame(ua, true, data)
			return
		}
		// Punch / ACK
		if isPunchProbe(data) {
			e.notePeerProbeTURN(ua)
			return
		}
		if isPunchAck(data) {
			e.notePeerAckTURN(ua)
			return
		}
		// IPv4 数据包（TUN）
		if len(data) > 0 && data[0]>>4 == 4 {
			e.notePeerTrafficTURN(ua)
			e.onRemotePacket(data)
			return
		}
	}

	e.relayMgr = NewRelayManager(ws, e.turnClient, e)
	e.relayMgr.Report(10 * time.Second)

	// ★ 新增：P2P stream manager
	e.p2pStreams = NewP2PStreamManager(e)

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
	e.startP2PKeepalive()

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

// ============ 超时降级 ============

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

// ★ 新增：P2PAvailable 超时降级。
//
// 打洞握手成功后进入 P2PAvailable，30 秒内如果没有升到 P2P（即没有
// 收到 PONG 或真实数据帧），强制降级到 TURN/WS。
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

// ============ 信令 ============

func (e *Edge) handleSignaling(msg map[string]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[信令] handleSignaling panic: %v\n%s", r, debug.Stack())
		}
	}()

	t, _ := msg["type"].(string)
	from, _ := msg["from"].(string)

	// Android WS 特有：用假消息触发重连后清理
	if t == "_reconnected" {
		log.Printf("[信令] WebSocket 重连成功，重新上报元数据")

		if e.relayMgr != nil {
			e.relayMgr.ClearAll()
		}
		if e.p2pStreams != nil {
			e.p2pStreams.closeAll()
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
				// ★ 新增：读 lanEndpoints
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
		// ★ 新增：读 lanEndpoints
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
			// ★ 服务端明确要求降级 → 用 ForceFallback，绕过 P2PAvailable 保护
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

func (e *Edge) maybeResetFailCountForPeer(peerID, newPubIP string, newPubPort int) {
	if peerID == "" || newPubIP == "" || newPubPort <= 0 {
		return
	}

	e.peersMu.RLock()
	oldPubIP := ""
	oldPubPort := 0
	if p, ok := e.peers[peerID]; ok {
		oldPubIP = p.PubIP
		oldPubPort = p.PubPort
	}
	e.peersMu.RUnlock()

	if oldPubIP == "" || (oldPubIP == newPubIP && oldPubPort == newPubPort) {
		return
	}

	natHoleActiveMu.Lock()
	hadCount := failCounts[peerID]
	delete(failCounts, peerID)
	natHoleActiveMu.Unlock()

	if e.relayMgr != nil {
		e.relayMgr.ClearPeer(peerID)
	}

	log.Printf("[NAT-HOLE] %s 出口变化 %s:%d → %s:%d，重置失败计数 (was=%d)",
		peerID, oldPubIP, oldPubPort, newPubIP, newPubPort, hadCount)
}

func (e *Edge) maybeResetOwnFailCounts(newEndpoint string) {
	if newEndpoint == "" {
		return
	}

	newIP := extractIPFromEndpoint(newEndpoint)
	if newIP == "" {
		return
	}

	e.mu.Lock()
	oldEndpoint := ""
	if e.natMeta != nil {
		oldEndpoint = e.natMeta.PublicEndpoint
	}
	e.mu.Unlock()

	oldIP := extractIPFromEndpoint(oldEndpoint)

	if oldIP == "" || oldIP == newIP {
		return
	}

	e.mu.Lock()
	if e.natMeta != nil {
		e.natMeta.PublicEndpoint = newEndpoint
		e.natMeta.P2PEndpoint = newEndpoint
	}
	e.mu.Unlock()

	natHoleActiveMu.Lock()
	count := len(failCounts)
	if count > 0 {
		failCounts = make(map[string]int)
	}
	natHoleActiveMu.Unlock()

	log.Printf("[NAT-HOLE] 本机出口 IP 变化 %s → %s，重置所有失败计数 (count=%d)",
		oldIP, newIP, count)
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

// reportMetadata 上报 p2p_metadata。
//
// ★ 改动：
//   1. 加 lanEndpoints 上报
//   2. 删掉 share_announce（服务端不处理，只浪费 WS 带宽）
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
	if len(nm.AllEndpoints) > 0 {
		metaPayload["assistedEndpoints"] = nm.AllEndpoints
	}

	// ★ 新增：LAN 地址上报（实时收集）
	vip := e.GetVirtualIP()
	lanEndpoints := collectLANEndpoints(e.udpPort, vip)
	if len(lanEndpoints) > 0 {
		metaPayload["lanEndpoints"] = lanEndpoints
	}

	log.Printf("[信令] 上报 p2p_metadata: natType=%s publicEndpoint=%q multiExit=%v assisted=%d lan=%d",
		nm.NATType, nm.PublicEndpoint, nm.MultiExit, len(nm.AllEndpoints), len(lanEndpoints))

	if ws == nil {
		return
	}
	_ = ws.Send(map[string]interface{}{"type": "p2p_metadata", "payload": metaPayload})
}

func jsonInt(v interface{}) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

// registerPeer 注册/更新一个 peer。
//
// ★ 改动：加 lanEndpoints 参数
func (e *Edge) registerPeer(pid, vip, pubIP string, pubPort int, natType string, assisted, lanEndpoints []string) {
	var udpAddr *net.UDPAddr
	if pubIP != "" && pubPort > 0 {
		udpAddr = &net.UDPAddr{IP: net.ParseIP(pubIP), Port: pubPort}
	}

	// LAN 按本机前缀亲和度排序（同 WiFi 优先）
	selfVIP := e.GetVirtualIP()
	prefixes := localPrefixes(selfVIP)
	sortedLAN := sortLANByAffinity(lanEndpoints, prefixes)

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
		if len(assisted) > 0 {
			p.AssistedEndpoints = assisted
		}
		if len(sortedLAN) > 0 {
			p.LanEndpoints = sortedLAN
		}
	} else {
		e.peers[pid] = &PeerInfo{
			ClientID:          pid,
			VirtualIP:         vip,
			PubIP:             pubIP,
			PubPort:           pubPort,
			UDPAddr:           udpAddr,
			NATType:           natType,
			AssistedEndpoints: assisted,
			LanEndpoints:      sortedLAN,
		}
	}
	e.peersMu.Unlock()

	if !existed {
		e.scheduleFallbackTimer(pid)
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

// runNatHole 执行一次打洞指令，成功时进入 P2PAvailable 并触发数据面探针。
func (e *Edge) runNatHole(instr *NatHoleInstruction) {
	res := e.executeNatHole(instr)
	if res == nil {
		return
	}

	if res.State == PunchStateSucceeded {
		// ★ 打洞握手成功 → P2PAvailable，同时发 P2P PING 探针
		e.relayMgr.MarkP2PAvailable(instr.TargetMac)
		if e.p2pStreams != nil {
			safeGo("p2p-probe", func() {
				e.p2pStreams.sendP2PProbe(instr.TargetMac)
			})
		}
		// 启动 30s 数据面验证窗口
		e.scheduleP2PAvailTimeout(instr.TargetMac)
	} else if res.State == PunchStateFailed {
		e.relayMgr.MarkFallback(instr.TargetMac)
	}

	// ★ p2pStatus 修正：TURN/Relay 报 0，只有 P2P/P2PAvailable 报 3/2
	p2pStatus := 0
	switch e.relayMgr.GetState(instr.TargetMac) {
	case ConnP2P:
		p2pStatus = 3
	case ConnP2PAvailable:
		p2pStatus = 2
	default:
		p2pStatus = 0
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

		// 1. STUN Binding Response
		if buf[0] == 0x01 && buf[1] == 0x01 {
			if ip, port, ok := parseSTUNResponse(buf[:n]); ok {
				e.addAssistedEndpoint(ip, port)
			}
			continue
		}

		// 2. N2NS 数据面（★ 新增，必须先于 Punch 检查）
		if isP2PFrame(buf[:n]) {
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.handleP2PFrame(ua, false, buf[:n])
			}
			continue
		}

		// 3. Punch / ACK
		if isPunchProbe(buf[:n]) {
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.notePeerProbe(ua)
			}
			continue
		}
		if isPunchAck(buf[:n]) {
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.notePeerAck(ua)
			}
			continue
		}

		// 4. IPv4 数据包（TUN）
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
			proto := 0
			if n >= 10 {
				proto = int(buf[9])
			}
			if proto == 17 && n < 200 {
				if e.ws != nil {
					_ = e.ws.SendBinary(buf[:n])
				}
			} else {
				e.logNoPeerThrottled(dstIP, n)
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

// ============ peer 探测处理（score 制） ============

func (e *Edge) notePeerProbe(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false, false, false)
}

func (e *Edge) notePeerAck(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false, false, true)
}

func (e *Edge) notePeerProbeTURN(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false, true, false)
}

func (e *Edge) notePeerAckTURN(addr *net.UDPAddr) {
	e.notePeerCommon(addr, false, true, true)
}

func (e *Edge) notePeerTraffic(addr *net.UDPAddr) {
	e.notePeerCommon(addr, true, false, false)
}

func (e *Edge) notePeerTrafficTURN(addr *net.UDPAddr) {
	e.notePeerCommon(addr, true, true, false)
}

// notePeerCommon 是打洞握手路径的统一入口。
//
// ★ 对齐 PC 端的 score 制匹配：
//   - p.UDPAddr         分数 3~4
//   - p.LanEndpoints    分数 2~5（★ 新增）
//   - p.AssistedEndpoints 分数 1~4（★ 新增）
//
// 规则：
//   1. Punch 与 ACK 分开处理，ACK 绝不回复
//   2. 收到 Punch 或 ACK 都升级到 P2PAvailable
//   3. 只有收到真实数据帧或 PONG 才升级到 P2P
//   4. 首次进入 P2PAvailable 时主动发 P2P PING + 启动超时降级
func (e *Edge) notePeerCommon(addr *net.UDPAddr, isRealData bool, viaTURN bool, isAck bool) {
	now := time.Now().UnixMilli()

	e.peersMu.Lock()
	var best *PeerInfo
	bestScore := 0
	addrIP := addr.IP
	addrPort := addr.Port

	for _, p := range e.peers {
		score := 0

		// 1. 匹配已记录的公网 UDP 地址
		if p.UDPAddr != nil && p.UDPAddr.IP.Equal(addrIP) {
			if p.UDPAddr.Port == addrPort {
				score = 4
			} else {
				score = 3
			}
		}

		// 2. 匹配 peer 的 LAN 地址
		for _, lan := range p.LanEndpoints {
			la, err := net.ResolveUDPAddr("udp", lan)
			if err != nil || la == nil || la.IP == nil {
				continue
			}
			if la.IP.Equal(addrIP) {
				if la.Port == addrPort {
					if score < 5 {
						score = 5
					}
				} else if score < 2 {
					score = 2
				}
			}
		}

		// 3. 匹配 peer 的 assisted 地址
		for _, ass := range p.AssistedEndpoints {
			aa, err := net.ResolveUDPAddr("udp", ass)
			if err != nil || aa == nil || aa.IP == nil {
				continue
			}
			if aa.IP.Equal(addrIP) {
				if aa.Port == addrPort {
					if score < 4 {
						score = 4
					}
				} else if score < 1 {
					score = 1
				}
			}
		}

		if score > 0 && score > bestScore {
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
	e.peersMu.Unlock()

	if e.relayMgr == nil {
		return
	}

	// ---- 1. 真实数据帧：直接升 P2P ----
	if isRealData {
		if e.relayMgr.GetState(clientID) != ConnP2P {
			log.Printf("[P2P] 从 %s (%s) 收到真实数据帧 → P2P 直连", clientID, ip)
			e.relayMgr.MarkP2P(clientID)
		}
		return
	}

	// ---- 2. Punch / ACK：打洞握手成功，升级到 P2PAvailable ----
	curState := e.relayMgr.GetState(clientID)
	justUpgraded := false
	if curState != ConnP2P && curState != ConnP2PAvailable {
		kind := "Punch"
		if isAck {
			kind = "ACK"
		}
		log.Printf("[P2P] 从 %s (%s) 收到 %s → P2PAvailable（等待数据帧验证）", clientID, ip, kind)
		e.relayMgr.MarkP2PAvailable(clientID)
		justUpgraded = true
	}

	if justUpgraded {
		if e.p2pStreams != nil {
			safeGo("p2p-probe", func() {
				e.p2pStreams.sendP2PProbe(clientID)
			})
		}
		e.scheduleP2PAvailTimeout(clientID)
	}

	// ---- 3. 只对 Punch 回 ACK，对 ACK 绝不回复 ----
	if isAck {
		return
	}
	if e.punchAckLimiter == nil || !e.punchAckLimiter.allow(clientID, time.Now()) {
		return
	}

	safeGo("punch-ack", func() {
		ack := buildPunchAck()
		e.writeProbe(addr, ack, viaTURN)
	})
}

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

// ============ 多 STUN 端点收集 ============

func (e *Edge) addAssistedEndpoint(ip string, port int) {
	if ip == "" || port <= 0 {
		return
	}
	ep := net.JoinHostPort(ip, strconv.Itoa(port))

	e.mu.Lock()
	if e.natMeta == nil {
		e.mu.Unlock()
		return
	}
	for _, existing := range e.natMeta.AllEndpoints {
		if existing == ep {
			e.mu.Unlock()
			return
		}
	}
	if len(e.natMeta.AllEndpoints) >= 5 {
		e.natMeta.AllEndpoints = e.natMeta.AllEndpoints[1:]
	}
	e.natMeta.AllEndpoints = append(e.natMeta.AllEndpoints, ep)
	count := len(e.natMeta.AllEndpoints)
	e.mu.Unlock()

	e.maybeResetOwnFailCounts(ep)

	log.Printf("[NAT] +assisted: %s (total=%d)", ep, count)
	e.reportMetadataThrottled()
}

func (e *Edge) reportMetadataThrottled() {
	now := time.Now().UnixMilli()
	last := atomic.LoadInt64(&e.lastMetadataReportAt)
	if now-last < 30000 {
		return
	}
	if !atomic.CompareAndSwapInt64(&e.lastMetadataReportAt, last, now) {
		return
	}
	e.reportMetadata()
}
