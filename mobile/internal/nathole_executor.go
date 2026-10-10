package internal

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

type NatHoleInstruction struct {
	Role                    int      `json:"role"`
	TTL                     int      `json:"ttl"`
	SendDelayMs             int      `json:"sendDelayMs"`
	PortsRangeFrom          uint32   `json:"portsRangeFrom"`
	PortsRangeTo            uint32   `json:"portsRangeTo"`
	TargetMac               string   `json:"targetMac"`
	TargetVirtualIp         string   `json:"targetVirtualIp"`
	TargetPubSocket         string   `json:"targetPubSocket"`
	TargetAssistedEndpoints []string `json:"targetAssistedEndpoints"`
	TargetLanEndpoints      []string `json:"targetLanEndpoints"`
	SenderMac               string   `json:"senderMac"`
	SenderP2PEndpoint       string   `json:"senderP2pEndpoint"`
	SenderPubSocket         string   `json:"senderPubSocket"`
	SenderNatType           string   `json:"senderNatType"`
	SenderBehavior          string   `json:"senderBehavior"`
	SenderAssistedEndpoints []string `json:"senderAssistedEndpoints"`
	ReceiverMac               string   `json:"receiverMac"`
	ReceiverP2PEndpoint       string   `json:"receiverP2pEndpoint"`
	ReceiverPubSocket         string   `json:"receiverPubSocket"`
	ReceiverNatType           string   `json:"receiverNatType"`
	ReceiverAssistedEndpoints []string `json:"receiverAssistedEndpoints"`
	PortsDifference    int  `json:"portsDifference"`
	RegularPortsChange bool `json:"regularPortsChange"`
	Mode               int  `json:"mode"`
	BehaviorIndex      int  `json:"behaviorIndex"`
}

type PunchResult struct {
	State         int    `json:"state"`
	Attempts      uint32 `json:"attempts"`
	Detail        string `json:"detail"`
	BehaviorIndex int    `json:"behaviorIndex"`
}

const (
	PunchStateNone       = 0
	PunchStateInProgress = 1
	PunchStateFailed     = 2
	PunchStateSucceeded  = 3
)

const maxConsecutiveFails = 5

var probePrefix = []byte{0x4E, 0x32, 0x4E, 0x50}

var (
	natHoleActiveMu sync.Mutex
	natHoleActive   map[string]bool
	failCounts      map[string]int
)

func init() {
	natHoleActive = make(map[string]bool)
	failCounts = make(map[string]int)
}

var fullScanTiers = []int{3, 10, 20, 30, 60, 100, 300, 1000, 3000, 10000}

func buildTiers(halfWidth int) []int {
	tiers := []int{}
	for _, t := range []int{3, 10, 20, 30, 60, 100, 300, 1000, 3000, 10000} {
		if t < halfWidth {
			tiers = append(tiers, t)
		}
	}
	tiers = append(tiers, halfWidth)
	return tiers
}

func extractPortFromEndpoint(ep string) int {
	if ep == "" {
		return 0
	}
	i := lastIndexByte(ep, ':')
	if i < 0 {
		return 0
	}
	port, err := strconv.Atoi(ep[i+1:])
	if err != nil || port < 1 || port > 65535 {
		return 0
	}
	return port
}

func lastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func (e *Edge) chooseScanTiers(targetAddr *net.UDPAddr) (tiers []int, sameStun bool, portDiff int, halfWidth int) {
	e.mu.Lock()
	ep := ""
	if e.natMeta != nil {
		ep = e.natMeta.PublicEndpoint
	}
	e.mu.Unlock()

	if ep == "" || targetAddr == nil {
		return fullScanTiers, false, 0, 0
	}

	myIP := extractIPFromEndpoint(ep)
	myPort := extractPortFromEndpoint(ep)
	targetIP := targetAddr.IP.String()
	targetPort := targetAddr.Port

	if myIP == "" || myIP != targetIP {
		return fullScanTiers, false, 0, 0
	}

	diff := myPort - targetPort
	if diff < 0 {
		diff = -diff
	}

	if diff >= 100 {
		return fullScanTiers, true, diff, 0
	}

	hw := diff + 100
	return buildTiers(hw), true, diff, hw
}

// startFailCountResetLoop 定期清空 failCounts。
//
// 触发场景：长时间降级到 TURN 后，网络环境可能已变化，应该给一次
// 重试机会。5 分钟清一次。
//
// 效果：
//   - 打洞失败 5 次 → failCounts 满 → 后续指令跳过（改用 TURN）
//   - 5 分钟后自动清空 → 服务端重新协调 → 客户端重新尝试打洞
//   - 若环境已恢复（NAT 漂移回原值 / 对端地址稳定），可能升回 P2P
//
// 副作用：每 5 分钟尝试一次（5~10 秒 CPU/流量），可接受。
func (e *Edge) startFailCountResetLoop() {
	safeGo("fail-count-reset", func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-e.doneCh:
				return
			case <-ticker.C:
				natHoleActiveMu.Lock()
				count := len(failCounts)
				if count > 0 {
					failCounts = make(map[string]int)
				}
				natHoleActiveMu.Unlock()
				if count > 0 {
					log.Printf("[NAT-HOLE] 定期清空 %d 个 target 的失败计数（5 分钟）", count)
				}
			}
		}
	})
}

func (e *Edge) executeNatHole(instr *NatHoleInstruction) *PunchResult {
	if e.relayMgr != nil && e.relayMgr.GetState(instr.TargetMac) == ConnP2P {
		log.Printf("[NAT-HOLE] 跳过指令 target=%s（已 P2P）", instr.TargetMac)
		return nil
	}

	natHoleActiveMu.Lock()
	fc := failCounts[instr.TargetMac]
	if fc >= maxConsecutiveFails {
		natHoleActiveMu.Unlock()
		log.Printf("[NAT-HOLE] 跳过指令 target=%s（已连续失败 %d 次）", instr.TargetMac, fc)
		return nil
	}
	if natHoleActive[instr.TargetMac] {
		natHoleActiveMu.Unlock()
		log.Printf("[NAT-HOLE] 跳过重复指令 target=%s（已在处理中）", instr.TargetMac)
		return nil
	}
	natHoleActive[instr.TargetMac] = true
	natHoleActiveMu.Unlock()
	defer func() {
		natHoleActiveMu.Lock()
		delete(natHoleActive, instr.TargetMac)
		natHoleActiveMu.Unlock()
	}()

	e.reportInProgress(instr)

	startAt := time.Now().UnixMilli()

	res := &PunchResult{
		State:         PunchStateInProgress,
		BehaviorIndex: instr.BehaviorIndex,
	}

	targetAddr := e.resolveTarget(instr)
	if targetAddr == nil {
		res.State = PunchStateFailed
		res.Detail = "无法解析目标地址"
		log.Printf("[NAT-HOLE] 目标地址解析失败 target=%s", instr.TargetPubSocket)
		return res
	}

	var prevTTL int = -1
	if instr.TTL > 0 && e.udpConn != nil {
		p := ipv4.NewPacketConn(e.udpConn)
		if cur, err := p.TTL(); err == nil {
			prevTTL = cur
		}
		if err := p.SetTTL(instr.TTL); err != nil {
			log.Printf("[NAT-HOLE] 设置 TTL=%d 失败: %v", instr.TTL, err)
		}
		defer func() {
			if prevTTL > 0 {
				_ = p.SetTTL(prevTTL)
			}
		}()
	}

	log.Printf(
		"[NAT-HOLE] 开始打洞 role=%d target=%s:%d rung=%d mode=%d ttl=%d",
		instr.Role, targetAddr.IP, targetAddr.Port,
		instr.BehaviorIndex, instr.Mode, instr.TTL,
	)

	var assistedTargets []*net.UDPAddr
	for _, ep := range instr.TargetAssistedEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			assistedTargets = append(assistedTargets, addr)
		}
	}

	if instr.Role == 0 && instr.SendDelayMs > 0 {
		time.Sleep(time.Duration(instr.SendDelayMs) * time.Millisecond)
	}

	probe := buildPunchProbe(e.GetVirtualIP())
	var attempts uint32

	tiers, sameStun, portDiff, halfWidth := e.chooseScanTiers(targetAddr)

	if sameStun && halfWidth > 0 {
		log.Printf("[NAT-HOLE] 模式：同 STUN IP，端口差=%d，精准扫描 targetPort±%d，分级 %v",
			portDiff, halfWidth, tiers)
	} else if sameStun {
		log.Printf("[NAT-HOLE] 模式：同 STUN IP 但端口差=%d（≥100），全端口扫描，分级 %v",
			portDiff, tiers)
	} else {
		log.Printf("[NAT-HOLE] 模式：不同 STUN IP，全端口扫描，分级 %v", tiers)
	}

	lastHalf := 0
	receiverPort := targetAddr.Port

	for _, half := range tiers {
		select {
		case <-e.doneCh:
			res.State = PunchStateFailed
			res.Detail = "aborted: edge stopped"
			return res
		default:
		}

		var tierTargets []*net.UDPAddr
		for port := receiverPort - half; port <= receiverPort+half; port++ {
			if port < 1 || port > 65535 {
				continue
			}
			if port >= receiverPort-lastHalf && port <= receiverPort+lastHalf {
				continue
			}
			tierTargets = append(tierTargets, &net.UDPAddr{
				IP:   targetAddr.IP,
				Port: port,
			})
		}

		if lastHalf == 0 && len(assistedTargets) > 0 {
			tierTargets = append(tierTargets, assistedTargets...)
		}

		rounds := 3
		if half > 500 {
			rounds = 1
		}

		success := false
		for r := 0; r < rounds; r++ {
			select {
			case <-e.doneCh:
				res.State = PunchStateFailed
				res.Detail = "aborted: edge stopped"
				return res
			default:
			}

			for _, t := range tierTargets {
				if _, err := e.udpConn.WriteTo(probe, t); err == nil {
					attempts++
				}
			}

			if e.hasTrafficFromTarget(instr.TargetMac, startAt) {
				success = true
				break
			}

			select {
			case <-e.doneCh:
				res.State = PunchStateFailed
				res.Detail = "aborted: edge stopped"
				return res
			case <-time.After(100 * time.Millisecond):
			}
		}

		if success {
			res.State = PunchStateSucceeded
			res.Attempts = attempts
			res.Detail = fmt.Sprintf("端口扫描成功 (tier=±%d)", half)
			e.recordP2PSuccess(instr, targetAddr)
			log.Printf("[NAT-HOLE] ✅ 成功 role=%d target=%s attempts=%d tier=±%d",
				instr.Role, targetAddr.IP, attempts, half)
			e.resetFailCount(instr.TargetMac)
			return res
		}

		log.Printf("[NAT-HOLE] tier=±%d 未命中（本轮 %d 个），扩大范围（累计 attempts=%d）",
			half, len(tierTargets), attempts)
		lastHalf = half
	}

	res.State = PunchStateFailed
	res.Attempts = attempts
	res.Detail = "无响应"
	log.Printf("[NAT-HOLE] ❌ 失败 role=%d target=%s attempts=%d",
		instr.Role, targetAddr.IP, attempts)

	natHoleActiveMu.Lock()
	failCounts[instr.TargetMac]++
	newFc := failCounts[instr.TargetMac]
	natHoleActiveMu.Unlock()
	if newFc >= maxConsecutiveFails {
		log.Printf("[NAT-HOLE] target=%s 连续失败 %d 次，后续指令将跳过（改用中继）",
			instr.TargetMac, newFc)
	}

	return res
}

func (e *Edge) resetFailCount(peerID string) {
	natHoleActiveMu.Lock()
	delete(failCounts, peerID)
	natHoleActiveMu.Unlock()
}

func (e *Edge) recordP2PSuccess(instr *NatHoleInstruction, targetAddr *net.UDPAddr) {
	e.peersMu.Lock()
	if p, ok := e.peers[instr.TargetMac]; ok {
		p.UDPAddr = targetAddr
		p.lastRecvAt = time.Now().UnixMilli()
	} else {
		e.peers[instr.TargetMac] = &PeerInfo{
			ClientID:   instr.TargetMac,
			VirtualIP:  instr.TargetVirtualIp,
			UDPAddr:    targetAddr,
			lastRecvAt: time.Now().UnixMilli(),
		}
	}
	e.peersMu.Unlock()
}

func (e *Edge) reportInProgress(instr *NatHoleInstruction) {
	if e.ws == nil {
		return
	}
	_ = e.ws.Send(map[string]interface{}{
		"type": "p2p_state_info",
		"payload": map[string]interface{}{
			"to": []map[string]interface{}{
				{
					"macAddr": instr.TargetMac,
					"punchResult": map[string]interface{}{
						"state":         PunchStateInProgress,
						"attempts":      0,
						"detail":        "round started",
						"behaviorIndex": instr.BehaviorIndex,
					},
					"punchResultPeerMac": instr.TargetMac,
				},
			},
		},
	})
}

func parseSockAddr(s string) *net.UDPAddr {
	if s == "" {
		return nil
	}
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil
	}
	return &net.UDPAddr{IP: ip, Port: port}
}

func (e *Edge) resolveTarget(instr *NatHoleInstruction) *net.UDPAddr {
	sock := instr.TargetPubSocket
	if sock == "" {
		if instr.Role == 1 {
			sock = instr.SenderPubSocket
		} else {
			sock = instr.ReceiverPubSocket
		}
	}
	return parseSockAddr(sock)
}

func buildPunchProbe(virtualIP string) []byte {
	buf := make([]byte, 32)
	copy(buf[0:4], probePrefix)
	if ip := net.ParseIP(virtualIP); ip != nil && ip.To4() != nil {
		copy(buf[4:8], ip.To4())
	}
	return buf
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

// hasTrafficFromAny 保留兼容（当前无调用方）。
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
