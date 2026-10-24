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
	lastPublicRecvAt  int64
	lastP2PRecvAt     int64 // ★ P2P 直连真实数据帧时间（viaTURN=false 且 isRealData=true）
	loggedReady       bool
	hasRealData       bool
	AssistedEndpoints []string
	LanEndpoints      []string
	Features          []string
	registeredAt      int64
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

	// ★ 网络变化去抖 / 合并延迟（见 edge_network.go）
	lastNetworkChangedAt int64
	lastNetworkType      string
	pendingNetChangeMu   sync.Mutex
	pendingNetChange     string
	pendingNetChangeT    *time.Timer

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

	lastDataFrameAt int64

	// peerID -> 上次上报的 p2pStatus
	p2pStatusReported   map[string]int
	p2pStatusReportedMu sync.Mutex

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

	log.Printf("[Keepalive] 启动，每 %v 刷新 %d 个 STUN 服务器（自适应：有数据时跳过）",
		keepaliveInterval, len(addrs))

	safeGo("keepalive", func() {
		sendOnce := func() {
			last := atomic.LoadInt64(&e.lastDataFrameAt)
			if last > 0 && time.Now().UnixMilli()-last < 10000 {
				return
			}
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

// ============ P2P 通道保活 + 活性检测 ============

const p2pKeepaliveInterval = 15 * time.Second
const p2pIdleKeepaliveInterval = 2 * time.Second
const p2pIdleDeadMs = 30000
const lanIdleDeadMs = 60000
const lanProbeIntervalMs = 30000

func (e *Edge) startP2PKeepalive() {
	log.Printf("[P2P-Keepalive] 启动：非 P2P 通道每 %v，P2P 通道每 %v（防 WiFi 省电）",
		p2pKeepaliveInterval, p2pIdleKeepaliveInterval)

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

	safeGo("p2p-idle-keepalive", func() {
		ticker := time.NewTicker(p2pIdleKeepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.doneCh:
				log.Printf("[P2P-Idle-Keepalive] 已停止")
				return
			case <-ticker.C:
				e.sendP2PIdleKeepalive()
			}
		}
	})
}

func (e *Edge) sendP2PKeepalive() {
	if e.relayMgr == nil || e.p2pStreams == nil {
		return
	}

	e.peersMu.RLock()
	var nonP2PPeers []string
	for id, p := range e.peers {
		if p.UDPAddr == nil {
			continue
		}
		st := e.relayMgr.GetState(id)
		if st == ConnLAN || st == ConnP2P || st == ConnP2PAvailable {
			continue
		}
		nonP2PPeers = append(nonP2PPeers, id)
	}
	e.peersMu.RUnlock()

	for _, id := range nonP2PPeers {
		e.p2pStreams.sendP2PProbe(id)
	}
}

// sendP2PIdleKeepalive P2P 状态发 probe + 无流量降级检测。
//
// ★ LAN 状态：每 30s 发一次 P2P probe（保活 + 探测），60s 无流量降级到 P2P。
// ★ P2P 状态：30s 无 **P2P 真实数据** 降级到 TURN/WS（用 lastP2PRecvAt，不用 lastRecvAt）。
// ★ P2PAvailable：交给 60s 超时处理。
func (e *Edge) sendP2PIdleKeepalive() {
	if e.relayMgr == nil || e.p2pStreams == nil {
		return
	}

	now := time.Now().UnixMilli()

	e.peersMu.RLock()
	var deadPeers []string
	var lanDeadPeers []string
	var lanProbePeers []string
	var p2pPeers []string
	for id, p := range e.peers {
		if p.UDPAddr == nil {
			continue
		}
		st := e.relayMgr.GetState(id)

		if st == ConnLAN {
			if p.lastRecvAt > 0 && now-p.lastRecvAt > lanProbeIntervalMs {
				lanProbePeers = append(lanProbePeers, id)
			}
			if p.lastRecvAt > 0 && now-p.lastRecvAt > lanIdleDeadMs {
				lanDeadPeers = append(lanDeadPeers, id)
			}
			continue
		}

		if st != ConnP2P && st != ConnP2PAvailable {
			continue
		}

		if st == ConnP2P && p.lastP2PRecvAt > 0 && now-p.lastP2PRecvAt > p2pIdleDeadMs {
			log.Printf("[P2P] %s P2P 真实数据 %ds 未更新（lastRecvAt 可能被 TURN 包刷新），判定 P2P 失效",
				id, (now-p.lastP2PRecvAt)/1000)
			deadPeers = append(deadPeers, id)
			continue
		}

		p2pPeers = append(p2pPeers, id)
	}
	e.peersMu.RUnlock()

	for _, id := range lanProbePeers {
		e.p2pStreams.sendP2PProbe(id)
	}

	for _, id := range lanDeadPeers {
		log.Printf("[LAN] %s 60s 无流量，降级到公网 P2P", id)
		e.relayMgr.DemoteLANToP2P(id, "no-traffic-60s")
	}

	for _, id := range deadPeers {
		log.Printf("[P2P] %s %ds 无 P2P 真实数据，判定 P2P 失效，强制降级",
			id, p2pIdleDeadMs/1000)
		e.relayMgr.ForceFallback(id, "no-traffic-30s")
		e.peersMu.Lock()
		if p := e.peers[id]; p != nil {
			p.lastP2PRecvAt = time.Now().UnixMilli()
		}
		e.peersMu.Unlock()
	}

	for _, id := range p2pPeers {
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
		cfg:               cfg,
		clientId:          clientId,
		nodeName:          nodeName,
		roomId:            cfg.RoomID,
		virtualCIDR:       "10.64.0.0/24",
		peers:             make(map[string]*PeerInfo),
		fallbackTimers:    make(map[string]*time.Timer),
		p2pAvailTimers:    make(map[string]*time.Timer),
		lastNoPeerLog:     make(map[string]int64),
		tunStateLog:       make(map[string]ConnType),
		p2pStatusReported: make(map[string]int),
		tunWriteCh:        make(chan []byte, 4096),
		doneCh:            make(chan struct{}),
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
			return nil, fmt.Errorf("绑定 UDP 失败: %v", err)
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
			if len(data) >= 4 && string(data[:4]) == "N2NA" {
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
		if err := e.socks5Srv.Listen(); err != nil {
			log.Printf("[SOCKS5] %v", err)
			e.Stop()
			return nil, err
		}
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
		if e.natMeta != nil && len(e.natMeta.AllEndpoints) > 0 {
			existing := make(map[string]bool, len(meta.AllEndpoints))
			for _, ep := range meta.AllEndpoints {
				existing[ep] = true
			}
			for _, ep := range e.natMeta.AllEndpoints {
				if !existing[ep] {
					meta.AllEndpoints = append(meta.AllEndpoints, ep)
					existing[ep] = true
				}
			}
		}
		e.natMeta = meta
		wsRef := e.ws
		mergedCount := len(meta.AllEndpoints)
		e.mu.Unlock()

		log.Printf("[NAT] 探测完成: %s pub=%s multiExit=%v endpoints=%d",
			meta.NATType, meta.PublicEndpoint, meta.MultiExit, mergedCount)

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

	// ★ 清理 pending network change timer
	e.pendingNetChangeMu.Lock()
	if e.pendingNetChangeT != nil {
		e.pendingNetChangeT.Stop()
		e.pendingNetChangeT = nil
	}
	e.pendingNetChange = ""
	e.pendingNetChangeMu.Unlock()

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

func (e *Edge) scheduleP2PAvailTimeout(peerID string) {
	if e.relayMgr == nil || peerID == "" {
		return
	}

	e.p2pAvailTimersMu.Lock()
	if t, ok := e.p2pAvailTimers[peerID]; ok {
		t.Stop()
	}

	t := time.AfterFunc(60*time.Second, func() {
		e.p2pAvailTimersMu.Lock()
		delete(e.p2pAvailTimers, peerID)
		e.p2pAvailTimersMu.Unlock()

		if e.relayMgr == nil {
			return
		}
		if e.relayMgr.GetState(peerID) == ConnP2PAvailable {
			log.Printf("[P2P] %s P2PAvailable 超时 60s 未验证数据面，强制降级", peerID)
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

		e.p2pStatusReportedMu.Lock()
		e.p2pStatusReported = make(map[string]int)
		e.p2pStatusReportedMu.Unlock()

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
		nActive := len(natHoleActive)
		if nActive > 0 {
			natHoleActive = make(map[string]bool)
		}
		natHoleActiveMu.Unlock()
		if failCount > 0 || nActive > 0 {
			log.Printf("[NAT-HOLE] WS 重连，清空 %d 个失败计数 / %d 个 in-flight 标记",
				failCount, nActive)
		}

		e.reportMetadata()
		atomic.StoreInt64(&e.lastMetadataReportAt, time.Now().UnixMilli())

		// ★ 补发 network_changed（防止切换瞬间丢失导致服务端 punchState 残留）
		if e.ws != nil {
			_ = e.ws.Send(map[string]interface{}{
				"type":   "network_changed",
				"reason": "ws-reconnected",
			})
			log.Printf("[Network] WS 重连，补发 network_changed (reason=ws-reconnected)")
		}

		// ★ 主动触发一次 STUN 探测（不等 5s 保活周期）
		safeGo("reconnect-stun-probe", func() {
			time.Sleep(300 * time.Millisecond)
			if e.udpConn == nil {
				return
			}
			sent := 0
			for _, server := range keepaliveServers {
				addr, err := net.ResolveUDPAddr("udp4", server)
				if err != nil {
					continue
				}
				probe := buildSTUNBindingRequest()
				if _, err := e.udpConn.WriteTo(probe, addr); err == nil {
					sent++
				}
			}
			log.Printf("[Network] WS 重连，主动触发 STUN 探测（%d 个服务器，不等 5s 保活）", sent)
		})

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
				var features []string
				if raw, ok := pm["features"].([]interface{}); ok {
					for _, v := range raw {
						if s, ok := v.(string); ok && s != "" {
							features = append(features, s)
						}
					}
				}

				if pid == "" || pip == "" {
					continue
				}

				e.maybeResetFailCountForPeer(pid, pubIP, pubPort)

				e.registerPeer(pid, pip, pubIP, pubPort, natType, assisted, lanEndpoints)
				e.peersMu.Lock()
				if pi, ok := e.peers[pid]; ok {
					if relayAddr != "" {
						pi.TurnRelayAddr = relayAddr
					}
					if len(features) > 0 {
						pi.Features = features
					}
				}
				e.peersMu.Unlock()
				log.Printf("[信令] 已有节点: %s vip=%s pub=%s:%d nat=%s assisted=%d lan=%d features=%v",
					pid, pip, pubIP, pubPort, natType, len(assisted), len(lanEndpoints), features)
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
		var features []string
		if raw, ok := payload["features"].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok && s != "" {
					features = append(features, s)
				}
			}
		}

		log.Printf("[信令] joined: from=%s vip=%s pub=%s:%d nat=%s assisted=%d lan=%d features=%v",
			from, pip, pubIP, pubPort, natType, len(assisted), len(lanEndpoints), features)

		if from != "" && pip != "" {
			e.maybeResetFailCountForPeer(from, pubIP, pubPort)

			e.registerPeer(from, pip, pubIP, pubPort, natType, assisted, lanEndpoints)
			e.peersMu.Lock()
			if pi, ok := e.peers[from]; ok {
				if relayAddr != "" {
					pi.TurnRelayAddr = relayAddr
				}
				if len(features) > 0 {
					pi.Features = features
				}
			}
			e.peersMu.Unlock()

			if e.p2pStreams != nil && e.relayMgr != nil {
				st := e.relayMgr.GetState(from)
				if st != ConnLAN && st != ConnP2P {
					targetID := from
					safeGo("p2p-preconnect", func() {
						time.Sleep(200 * time.Millisecond)
						e.p2pStreams.sendP2PProbe(targetID)
						log.Printf("[P2P] 预连接试探 → %s", targetID)
					})
				}
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

		if e.relayMgr != nil {
			st := e.relayMgr.GetState(instr.TargetMac)
			if st == ConnLAN || st == ConnP2P {
				log.Printf("[NAT-HOLE] 跳过指令 target=%s（已 %s），上报 p2p_status=3",
					instr.TargetMac, st)
				e.reportP2PStatus(instr.TargetMac, 3)
				return
			}
			if st == ConnP2PAvailable {
				log.Printf("[NAT-HOLE] 跳过指令 target=%s（已 P2PAvailable），上报 p2p_status=2",
					instr.TargetMac)
				e.reportP2PStatus(instr.TargetMac, 2)
				return
			}
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
			st := e.relayMgr.GetState(peerID)
			if st == ConnLAN || st == ConnP2P {
				log.Printf("[信令] %s 已 %s，忽略 force_fallback，上报 p2p_status=3", peerID, st)
				e.reportP2PStatus(peerID, 3)
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

	case "connection_status":
		return

	case "peer_network_changed":
		reason, _ := msg["reason"].(string)
		log.Printf("[Network] %s 网络变化 (%s)，重置与其相关的状态", from, reason)

		if reason == "ws-reconnected" {
			e.peersMu.Lock()
			if p := e.peers[from]; p != nil {
				p.lastRecvAt = time.Now().UnixMilli()
			}
			e.peersMu.Unlock()
			log.Printf("[Network] %s ws-reconnected，保留 P2P/LAN 状态", from)
			return
		}

		if e.relayMgr != nil {
			e.relayMgr.ClearPeer(from)
		}

		e.peersMu.Lock()
		if p := e.peers[from]; p != nil {
			p.UDPAddr = nil
			p.lastLANRecvAt = 0
			p.lastPublicRecvAt = 0
			p.lastP2PRecvAt = 0
			p.lastRecvAt = 0
			p.PubIP = ""
			p.PubPort = 0
			p.TurnRelayAddr = ""
			p.AssistedEndpoints = nil
			p.LanEndpoints = nil
		}
		e.peersMu.Unlock()

		natHoleActiveMu.Lock()
		delete(failCounts, from)
		delete(natHoleActive, from)
		natHoleActiveMu.Unlock()

		e.cancelFallbackTimer(from)
		e.cancelP2PAvailTimeout(from)
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

		e.p2pStatusReportedMu.Lock()
		delete(e.p2pStatusReported, from)
		e.p2pStatusReportedMu.Unlock()

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

	if oldPubIP != newPubIP {
		if e.relayMgr != nil {
			e.relayMgr.ClearPeer(peerID)
		}
		log.Printf("[NAT-HOLE] %s 出口 IP 变化 %s → %s，清空连接状态 (was=%d)",
			peerID, oldPubIP, newPubIP, hadCount)
	} else {
		log.Printf("[NAT-HOLE] %s 端口漂移 %s:%d → %s:%d，保留 P2P 状态",
			peerID, oldPubIP, oldPubPort, newPubIP, newPubPort)
	}
}

// maybeResetOwnFailCounts 本机出口 IP 变化时重置失败计数。
//
// ★ 修复：oldIP == "" 时不 return，而是更新 PublicEndpoint。
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

	if oldIP == newIP {
		return
	}

	e.mu.Lock()
	if e.natMeta != nil {
		e.natMeta.PublicEndpoint = newEndpoint
		e.natMeta.P2PEndpoint = newEndpoint
	}
	e.mu.Unlock()

	if oldIP != "" {
		natHoleActiveMu.Lock()
		count := len(failCounts)
		if count > 0 {
			failCounts = make(map[string]int)
		}
		natHoleActiveMu.Unlock()

		log.Printf("[NAT-HOLE] 本机出口 IP 变化 %s → %s，重置所有失败计数 (count=%d)",
			oldIP, newIP, count)
	} else {
		log.Printf("[NAT-HOLE] 本机出口 IP 首次设置: %s（换网络后）", newIP)
	}
}

// findPeerCI 大小写不敏感查找 peer。
func (e *Edge) findPeerCI(peerID string) *PeerInfo {
	if peerID == "" {
		return nil
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	if p, ok := e.peers[peerID]; ok {
		return p
	}
	needle := strings.ToLower(peerID)
	for k, p := range e.peers {
		if strings.ToLower(k) == needle {
			return p
		}
	}
	return nil
}

func (e *Edge) peerExists(peerID string) bool {
	return e.findPeerCI(peerID) != nil
}

// registerPeer 处理三种 UDPAddr 切换场景。
func (e *Edge) registerPeer(pid, vip, pubIP string, pubPort int, natType string, assisted, lanEndpoints []string) {
	var udpAddr *net.UDPAddr
	if pubIP != "" && pubPort > 0 {
		udpAddr = &net.UDPAddr{IP: net.ParseIP(pubIP), Port: pubPort}
	}

	selfVIP := e.GetVirtualIP()
	prefixes := localPrefixes(selfVIP)
	sortedLAN := sortLANByAffinity(lanEndpoints, prefixes)

	e.peersMu.Lock()
	_, existed := e.peers[pid]
	if existed {
		p := e.peers[pid]

		oldPubIP := p.PubIP
		pubIpChanged := oldPubIP != "" && pubIP != "" && oldPubIP != pubIP
		pubIpFirstSet := oldPubIP == "" && pubIP != ""

		needSwitchToPublic := pubIpChanged
		if pubIpFirstSet && udpAddr != nil {
			selfOnLAN := false
			for _, lan := range sortedLAN {
				ra, err := net.ResolveUDPAddr("udp", lan)
				if err != nil || ra == nil || ra.IP == nil {
					continue
				}
				for _, pfx := range prefixes {
					if pfx.Contains(ra.IP) {
						selfOnLAN = true
						break
					}
				}
				if selfOnLAN {
					break
				}
			}
			if !selfOnLAN {
				needSwitchToPublic = true
				log.Printf("[P2P] %s 首次有公网 (pub=%s)，本机不在其 LAN，UDPAddr 切到公网",
					pid, pubIP)
			} else {
				log.Printf("[P2P] %s 首次有公网 (pub=%s)，但本机在其 LAN 上，保留 LAN",
					pid, pubIP)
			}
		}

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
			oldIsLAN := p.UDPAddr != nil && isPrivateIPv4(p.UDPAddr.IP)
			if !oldIsLAN || needSwitchToPublic {
				p.UDPAddr = udpAddr
				if pubIpChanged {
					log.Printf("[P2P] %s 出口 IP 变化 %s → %s，UDPAddr 切到新公网: %s",
						pid, oldPubIP, pubIP, udpAddr)
				}
			}
		}

		if natType != "" {
			p.NATType = natType
		}
		if len(assisted) > 0 {
			p.AssistedEndpoints = assisted
		}

		if len(sortedLAN) > 0 {
			p.LanEndpoints = sortedLAN
		} else if p.LanEndpoints != nil && len(p.LanEndpoints) > 0 {
			log.Printf("[P2P] %s joined 时 lan=0，清空本地 LanEndpoints（旧值: %v）",
				pid, p.LanEndpoints)
			p.LanEndpoints = nil

			if p.UDPAddr != nil && isPrivateIPv4(p.UDPAddr.IP) {
				if pubIP != "" && pubPort > 0 {
					newAddr := &net.UDPAddr{IP: net.ParseIP(pubIP), Port: pubPort}
					log.Printf("[P2P] %s joined lan=0 且 UDPAddr 是私网，切到公网: %s → %s",
						pid, p.UDPAddr, newAddr)
					p.UDPAddr = newAddr
				} else {
					log.Printf("[P2P] %s joined lan=0 且 UDPAddr 是私网，但公网地址未知，清空 UDPAddr",
						pid)
					p.UDPAddr = nil
				}
			}

			if e.relayMgr != nil && e.relayMgr.GetState(pid) == ConnLAN {
				e.relayMgr.ForceFallback(pid, "lan-endpoints-cleared")
			}
		}
	} else {
		var initialUDP *net.UDPAddr
		if len(sortedLAN) > 0 {
			for _, lan := range sortedLAN {
				if ra, err := net.ResolveUDPAddr("udp", lan); err == nil && ra != nil && ra.IP != nil {
					initialUDP = ra
					log.Printf("[P2P] %s 新建 peer，初始 UDPAddr 用 LAN: %s", pid, lan)
					break
				}
			}
		}
		if initialUDP == nil {
			initialUDP = udpAddr
		}

		e.peers[pid] = &PeerInfo{
			ClientID:          pid,
			VirtualIP:         vip,
			PubIP:             pubIP,
			PubPort:           pubPort,
			UDPAddr:           initialUDP,
			NATType:           natType,
			AssistedEndpoints: assisted,
			LanEndpoints:      sortedLAN,
			registeredAt:      time.Now().UnixMilli(),
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

	selfVIP := e.GetVirtualIP()
	prefixes := localPrefixes(selfVIP)

	e.peersMu.Lock()
	defer e.peersMu.Unlock()
	if p, ok := e.peers[instr.TargetMac]; ok {
		if udpAddr != nil && p.UDPAddr == nil {
			p.UDPAddr = udpAddr
		}

		if len(instr.TargetLanEndpoints) > 0 && len(p.LanEndpoints) == 0 {
			p.LanEndpoints = sortLANByAffinity(instr.TargetLanEndpoints, prefixes)
			log.Printf("[P2P] %s 从打洞指令补充 LanEndpoints: %v",
				instr.TargetMac, p.LanEndpoints)
		}

		if instr.TargetVirtualIp != "" {
			p.VirtualIP = instr.TargetVirtualIp
		}
	} else {
		var initialUDP *net.UDPAddr
		if len(instr.TargetLanEndpoints) > 0 {
			for _, lan := range instr.TargetLanEndpoints {
				if ra, err := net.ResolveUDPAddr("udp", lan); err == nil && ra != nil && ra.IP != nil {
					initialUDP = ra
					log.Printf("[P2P] %s 新建 peer（ensureTargetPeer），初始 UDPAddr 用 LAN: %s",
						instr.TargetMac, lan)
					break
				}
			}
		}
		if initialUDP == nil {
			initialUDP = udpAddr
		}

		e.peers[instr.TargetMac] = &PeerInfo{
			ClientID:     instr.TargetMac,
			VirtualIP:    instr.TargetVirtualIp,
			UDPAddr:      initialUDP,
			LanEndpoints: sortLANByAffinity(instr.TargetLanEndpoints, prefixes),
			registeredAt: time.Now().UnixMilli(),
		}
	}
}

func (e *Edge) runNatHole(instr *NatHoleInstruction) {
	res := e.executeNatHole(instr)
	if res == nil {
		return
	}

	if res.State == PunchStateSucceeded {
		curState := e.relayMgr.GetState(instr.TargetMac)
		if curState != ConnLAN {
			e.relayMgr.MarkP2PAvailable(instr.TargetMac)
			if e.p2pStreams != nil {
				safeGo("p2p-probe", func() {
					e.p2pStreams.sendP2PProbe(instr.TargetMac)
				})
			}
			e.scheduleP2PAvailTimeout(instr.TargetMac)
		}
	} else if res.State == PunchStateFailed {
		e.relayMgr.MarkFallback(instr.TargetMac)
	}

	p2pStatus := 0
	switch e.relayMgr.GetState(instr.TargetMac) {
	case ConnLAN:
		p2pStatus = 3
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

// reportP2PStatus 上报 p2pStatus 给服务端。
//
// ★ 根据 status 传不同 state：
//   status=3（P2P/LAN）→ state=3（成功）
//   status=2（P2PAvailable）→ state=1（InProgress）
//   status=0（降级到 TURN/WS）→ state=2（失败，服务端清 anySucceeded）
func (e *Edge) reportP2PStatus(peerID string, status int) {
	if e.ws == nil || peerID == "" {
		return
	}

	e.p2pStatusReportedMu.Lock()
	last, exists := e.p2pStatusReported[peerID]
	if exists && last == status {
		e.p2pStatusReportedMu.Unlock()
		return
	}
	e.p2pStatusReported[peerID] = status
	e.p2pStatusReportedMu.Unlock()

	var state int
	var detail string
	switch status {
	case 3:
		state = 3
		detail = "already-p2p"
	case 2:
		state = 1
		detail = "already-p2p-avail"
	case 0:
		state = 2
		detail = "degraded"
	default:
		state = 1
		detail = "unknown-status"
	}

	_ = e.ws.Send(map[string]interface{}{
		"type": "p2p_state_info",
		"payload": map[string]interface{}{
			"to": []map[string]interface{}{{
				"macAddr": peerID,
				"punchResult": map[string]interface{}{
					"state":    state,
					"attempts": 0,
					"detail":   detail,
				},
				"punchResultPeerMac": peerID,
				"p2pStatus":          status,
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

		if buf[0] == 0x01 && buf[1] == 0x01 {
			if ip, port, ok := parseSTUNResponse(buf[:n]); ok {
				e.addAssistedEndpoint(ip, port)
			}
			continue
		}

		if isP2PFrame(buf[:n]) {
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.handleP2PFrame(ua, false, buf[:n])
			}
			continue
		}

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

		if buf[0]>>4 == 4 {
			if ua, ok := addr.(*net.UDPAddr); ok {
				e.notePeerTraffic(ua)
			}
			e.onRemotePacket(buf[:n])
		}
	}
}

// tunReadLoop TUN 读循环。TCP / UDP / ICMP 全部走 SendToPeer。
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

		proto := 0
		if n >= 10 {
			proto = int(buf[9])
		}

		if target == nil {
			if e.ws != nil {
				_ = e.ws.SendBinary(buf[:n])
				atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
			} else {
				e.logNoPeerThrottled(dstIP, n)
			}
			continue
		}

		// ICMP 走 sendICMPDirect（严格按状态机走）
		if proto == 1 {
			e.sendICMPDirect(target, buf[:n])
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
			if n >= 10 {
				actualState := ConnUnknown
				if e.relayMgr != nil {
					actualState = e.relayMgr.GetState(target.ClientID)
				}
				e.logTunForward(target.ClientID, dstIP, n, proto, true, actualState)
			}
			continue
		}

		state := ConnUnknown
		if e.relayMgr != nil {
			state = e.relayMgr.GetState(target.ClientID)
		}

		if state == ConnTURN {
			pktCopy := make([]byte, n)
			copy(pktCopy, buf[:n])
			targetID := target.ClientID
			targetCopy := target
			pktLen := n
			protoCopy := proto

			safeGo("tun-send-turn", func() {
				sent := false
				if e.relayMgr != nil {
					sent = e.relayMgr.SendToPeer(targetID, pktCopy, targetCopy)
				}
				e.logTunForward(targetID, dstIP, pktLen, protoCopy, sent, ConnTURN)
				if sent {
					atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
				}
				if !sent && e.ws != nil {
					frame := buildN2NWFrame(targetID, pktCopy)
					_ = e.ws.SendBinary(frame)
				}
			})
			continue
		}

		sent := false
		if e.relayMgr != nil {
			sent = e.relayMgr.SendToPeer(target.ClientID, buf[:n], target)
		}
		if sent {
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
		}
		if n >= 10 {
			e.logTunForward(target.ClientID, dstIP, n, proto, sent, state)
		}
		if !sent && e.ws != nil {
			frame := buildN2NWFrame(target.ClientID, buf[:n])
			_ = e.ws.SendBinary(frame)
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
		}
	}
}

func (e *Edge) selfIsOnTargetLAN(target *PeerInfo) bool {
	if target == nil || len(target.LanEndpoints) == 0 {
		return false
	}
	myPrefixes := localPrefixes(e.getVirtualIPLocked())
	if len(myPrefixes) == 0 {
		return false
	}
	for _, lan := range target.LanEndpoints {
		ra, err := net.ResolveUDPAddr("udp", lan)
		if err != nil || ra == nil || ra.IP == nil {
			continue
		}
		for _, p := range myPrefixes {
			if p.Contains(ra.IP) {
				return true
			}
		}
	}
	return false
}

// sendICMPDirect ICMP 发送，严格按状态机走（与 Termux 的 sendICMP 一致）。
//
// ★ 修复：与 TCP/UDP 一致，ICMP 严格按 relayMgr 状态走。
//
//   - ConnTURN:       TURN relay → 失败走 WS
//   - ConnRelay:      WS
//   - ConnLAN:        LAN → 私网 UDPAddr → 公网 UDPAddr → WS（不降级状态）
//   - ConnP2P/avail:  selfOnLAN 时优先 LAN → UDPAddr → WS
//   - ConnUnknown:    兜底 LAN → UDPAddr → TURN → WS（不改状态）
//
// 与 TCP/UDP 的差异：ICMP 不参与 failCounts 降级。它是应答包，单次
// 发送失败不应影响 TCP/UDP 的状态。LAN 状态下 LAN 失败也不主动降级
// 到 P2PAvailable——让 sendP2PIdleKeepalive 的 60s 无流量检测去降级。
func (e *Edge) sendICMPDirect(target *PeerInfo, pkt []byte) {
	if target == nil {
		return
	}

	if e.relayMgr == nil {
		e.sendICMPBestEffort(target, pkt)
		return
	}

	state := e.relayMgr.GetState(target.ClientID)

	switch state {
	case ConnTURN:
		if target.TurnRelayAddr != "" && e.turnClient != nil && e.turnClient.IsReady() {
			relayAddr, err := net.ResolveUDPAddr("udp", target.TurnRelayAddr)
			if err == nil {
				if sendErr := e.turnClient.Send(pkt, relayAddr); sendErr == nil {
					atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
					return
				}
			}
		}
		if e.ws != nil {
			frame := buildN2NWFrame(target.ClientID, pkt)
			_ = e.ws.SendBinary(frame)
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
		}

	case ConnRelay:
		if e.ws != nil {
			frame := buildN2NWFrame(target.ClientID, pkt)
			_ = e.ws.SendBinary(frame)
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
		}

	case ConnLAN:
		sent := false
		if e.udpConn != nil && len(target.LanEndpoints) > 0 {
			for _, lan := range target.LanEndpoints {
				ra, err := net.ResolveUDPAddr("udp", lan)
				if err != nil || ra == nil || ra.IP == nil {
					continue
				}
				if _, err := e.udpConn.WriteTo(pkt, ra); err == nil {
					sent = true
					break
				}
			}
		}
		if !sent && target.UDPAddr != nil && isPrivateIPv4(target.UDPAddr.IP) && e.udpConn != nil {
			if _, err := e.udpConn.WriteTo(pkt, target.UDPAddr); err == nil {
				sent = true
			}
		}
		if sent {
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
			return
		}
		if target.UDPAddr != nil && !isPrivateIPv4(target.UDPAddr.IP) && e.udpConn != nil {
			if _, err := e.udpConn.WriteTo(pkt, target.UDPAddr); err == nil {
				atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
				return
			}
		}
		if e.ws != nil {
			frame := buildN2NWFrame(target.ClientID, pkt)
			_ = e.ws.SendBinary(frame)
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
		}

	case ConnP2P, ConnP2PAvailable:
		selfOnLAN := e.selfIsOnTargetLAN(target)
		sent := false

		if selfOnLAN && e.udpConn != nil && len(target.LanEndpoints) > 0 {
			for _, lan := range target.LanEndpoints {
				ra, err := net.ResolveUDPAddr("udp", lan)
				if err != nil || ra == nil || ra.IP == nil {
					continue
				}
				if _, err := e.udpConn.WriteTo(pkt, ra); err == nil {
					sent = true
					break
				}
			}
		}
		if !sent && target.UDPAddr != nil && e.udpConn != nil {
			skip := false
			if isPrivateIPv4(target.UDPAddr.IP) && !selfOnLAN {
				skip = true
				log.Printf("[ICMP] %s UDPAddr=%s 是私网且本机不在该 LAN，跳过（避免黑洞）",
					target.ClientID, target.UDPAddr)
			}
			if !skip {
				if _, err := e.udpConn.WriteTo(pkt, target.UDPAddr); err == nil {
					sent = true
				}
			}
		}
		if sent {
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
			return
		}
		if e.ws != nil {
			frame := buildN2NWFrame(target.ClientID, pkt)
			_ = e.ws.SendBinary(frame)
			atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
		}

	default:
		e.sendICMPBestEffort(target, pkt)
	}
}

// sendICMPBestEffort ICMP 兜底发送（状态 Unknown 或 relayMgr 未就绪时）。
//
// 按 LAN → UDPAddr → TURN → WS 顺序尝试，不改 relayMgr 状态。
func (e *Edge) sendICMPBestEffort(target *PeerInfo, pkt []byte) {
	if target == nil {
		return
	}

	selfOnLAN := e.selfIsOnTargetLAN(target)

	// 1. LAN
	if selfOnLAN && e.udpConn != nil && len(target.LanEndpoints) > 0 {
		for _, lan := range target.LanEndpoints {
			ra, err := net.ResolveUDPAddr("udp", lan)
			if err != nil || ra == nil || ra.IP == nil {
				continue
			}
			if _, err := e.udpConn.WriteTo(pkt, ra); err == nil {
				atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
				return
			}
		}
	}

	// 2. UDPAddr（私网跳过，除非本机也在该 LAN）
	if target.UDPAddr != nil && e.udpConn != nil {
		skip := false
		if isPrivateIPv4(target.UDPAddr.IP) && !selfOnLAN {
			skip = true
		}
		if !skip {
			if _, err := e.udpConn.WriteTo(pkt, target.UDPAddr); err == nil {
				atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
				return
			}
		}
	}

	// 3. TURN
	if target.TurnRelayAddr != "" && e.turnClient != nil && e.turnClient.IsReady() {
		relayAddr, err := net.ResolveUDPAddr("udp", target.TurnRelayAddr)
		if err == nil {
			if sendErr := e.turnClient.Send(pkt, relayAddr); sendErr == nil {
				atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
				return
			}
		}
	}

	// 4. WS
	if e.ws != nil {
		frame := buildN2NWFrame(target.ClientID, pkt)
		_ = e.ws.SendBinary(frame)
		atomic.StoreInt64(&e.lastDataFrameAt, time.Now().UnixMilli())
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

// notePeerCommon 统一处理所有来源的 UDP 包。
//
// ★ 修复：
//   - isRealData && !viaTURN → 更新 lastP2PRecvAt
//   - viaTURN && cur == ConnUnknown → MarkFallback（面板不再显示 --）
//   - viaTURN && cur == ConnLAN/ConnP2P → ForceFallback("peer-via-turn")
func (e *Edge) notePeerCommon(addr *net.UDPAddr, isRealData bool, viaTURN bool, isAck bool) {
	now := time.Now().UnixMilli()

	e.peersMu.Lock()
	var best *PeerInfo
	bestScore := 0
	addrIP := addr.IP
	addrPort := addr.Port

	for _, p := range e.peers {
		score := 0

		if p.UDPAddr != nil && p.UDPAddr.IP.Equal(addrIP) {
			if p.UDPAddr.Port == addrPort {
				score = 4
			} else {
				score = 3
			}
		}

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

	newIsLAN := isPrivateIPv4(addr.IP)
	oldIsLAN := best.UDPAddr != nil && isPrivateIPv4(best.UDPAddr.IP)

	shouldUpdate := false
	if best.UDPAddr == nil {
		shouldUpdate = true
	} else if !oldIsLAN && newIsLAN {
		shouldUpdate = true
	} else if oldIsLAN && !newIsLAN {
		lastLAN := best.lastLANRecvAt
		if lastLAN == 0 {
			lastLAN = best.registeredAt
		}
		if lastLAN > 0 && now-lastLAN > 30000 {
			shouldUpdate = true
			log.Printf("[P2P] %s (note) LAN 无流量（lastLAN=%ds 前），改用公网地址",
				best.ClientID, (now-lastLAN)/1000)
		}
	} else if oldIsLAN && newIsLAN {
		if best.UDPAddr.IP.Equal(addr.IP) && best.UDPAddr.Port != addrPort {
			shouldUpdate = true
		}
	} else {
		if best.UDPAddr.Port != addrPort {
			shouldUpdate = true
		}
	}

	if newIsLAN {
		best.lastLANRecvAt = now
	}

	if shouldUpdate {
		best.UDPAddr = &net.UDPAddr{IP: addr.IP, Port: addrPort}
	}

	best.lastRecvAt = now
	if !newIsLAN {
		best.lastPublicRecvAt = now
	}
	if isRealData && !viaTURN {
		best.lastP2PRecvAt = now
	}
	if isRealData {
		best.hasRealData = true
	}
	clientID := best.ClientID
	ip := addr.IP.String()
	e.peersMu.Unlock()

	if e.relayMgr == nil {
		return
	}

	if isRealData {
		cur := e.relayMgr.GetState(clientID)

		if viaTURN {
			if cur == ConnLAN || cur == ConnP2P {
				log.Printf("[TURN] 从 %s (%s) 收到真实数据帧（via TURN）→ 对端已降级，本机跟进",
					clientID, ip)
				e.relayMgr.ForceFallback(clientID, "peer-via-turn")
			} else if cur == ConnUnknown {
				log.Printf("[TURN] 从 %s (%s) 收到真实数据帧（Unknown → TURN）",
					clientID, ip)
				e.relayMgr.MarkFallback(clientID)
			}
			return
		}

		if newIsLAN {
			if cur != ConnLAN {
				log.Printf("[LAN] 从 %s (%s) 收到真实数据帧 → LAN 直连", clientID, ip)
				e.relayMgr.MarkLAN(clientID)
			}
		} else {
			if cur != ConnP2P {
				log.Printf("[P2P] 从 %s (%s) 收到真实数据帧 → P2P 直连", clientID, ip)
				e.relayMgr.MarkP2P(clientID)
			}
		}
		return
	}

	curState := e.relayMgr.GetState(clientID)

	if newIsLAN {
		if curState != ConnLAN {
			kind := "Punch"
			if isAck {
				kind = "ACK"
			}
			log.Printf("[LAN] 从 %s (%s) 收到 %s → LAN 直连", clientID, ip, kind)
			e.relayMgr.MarkLAN(clientID)
		}
	} else {
		justUpgraded := false
		if curState != ConnLAN && curState != ConnP2P && curState != ConnP2PAvailable {
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
	}

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

// hasTrafficFromTarget 检测是否收到对端任何包（LAN 或公网）
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

// hasTrafficFromTargetLAN 检测是否收到对端 LAN 包
func (e *Edge) hasTrafficFromTargetLAN(peerID string, since int64) bool {
	if peerID == "" {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	p := e.peers[peerID]
	if p == nil {
		return false
	}
	return p.lastLANRecvAt >= since
}

// hasTrafficFromTargetPublic 检测是否收到对端公网包
func (e *Edge) hasTrafficFromTargetPublic(peerID string, since int64) bool {
	if peerID == "" {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	p := e.peers[peerID]
	if p == nil {
		return false
	}
	return p.lastPublicRecvAt >= since
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

// addAssistedEndpoint 添加 STUN 探测到的辅助端点。
//
// ★ 修复：wasEmpty || ipChanged 都立即上报，绕过 30 秒节流。
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
	oldPublicEndpoint := e.natMeta.PublicEndpoint
	wasEmpty := oldPublicEndpoint == ""
	e.mu.Unlock()

	e.maybeResetOwnFailCounts(ep)

	e.mu.Lock()
	newPubIP := ""
	oldPubIP := extractIPFromEndpoint(oldPublicEndpoint)
	if e.natMeta != nil {
		newPubIP = extractIPFromEndpoint(e.natMeta.PublicEndpoint)
	}
	e.mu.Unlock()

	ipChanged := oldPubIP != "" && newPubIP != "" && oldPubIP != newPubIP

	log.Printf("[NAT] +assisted: %s (total=%d, wasEmpty=%v, ipChanged=%v, %s→%s)",
		ep, count, wasEmpty, ipChanged, oldPubIP, newPubIP)

	if wasEmpty || ipChanged {
		atomic.StoreInt64(&e.lastMetadataReportAt, 0)
		e.reportMetadata()
		if ipChanged {
			log.Printf("[NAT] PublicEndpoint IP 变化 %s → %s，立即上报 p2p_metadata",
				oldPubIP, newPubIP)
		} else {
			log.Printf("[NAT] PublicEndpoint 首次设置，立即上报 p2p_metadata")
		}
	} else {
		e.reportMetadataThrottled()
	}
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
		"features":           []string{"n2na", "n2nw"},
	}
	if nm.PublicEndpoint != "" {
		metaPayload["publicEndpoint"] = nm.PublicEndpoint
	}
	if len(nm.AllEndpoints) > 0 {
		metaPayload["assistedEndpoints"] = nm.AllEndpoints
	}

	vip := e.GetVirtualIP()
	lanEndpoints := collectLANEndpoints(e.udpPort, vip)
	if len(lanEndpoints) > 0 {
		metaPayload["lanEndpoints"] = lanEndpoints
	}

	// 上报 lanSubnets（服务端 shouldShareLan 判 CIDR 重叠）
	if prefixes := localPrefixes(vip); len(prefixes) > 0 {
		cidrs := make([]string, 0, len(prefixes))
		for _, n := range prefixes {
			cidrs = append(cidrs, n.String())
		}
		metaPayload["lanSubnets"] = cidrs
	}

	subnetCount := 0
	if s, ok := metaPayload["lanSubnets"].([]string); ok {
		subnetCount = len(s)
	}
	log.Printf("[信令] 上报 p2p_metadata: natType=%s publicEndpoint=%q multiExit=%v assisted=%d lan=%d subnets=%d features=[n2na,n2nw]",
		nm.NATType, nm.PublicEndpoint, nm.MultiExit, len(nm.AllEndpoints), len(lanEndpoints), subnetCount)

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
