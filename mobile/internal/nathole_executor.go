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
	SenderLanEndpoints      []string `json:"senderLanEndpoints"`
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

	if diff > 5000 {
		return nil, true, diff, -1
	}

	if diff >= 100 {
		return fullScanTiers, true, diff, 0
	}

	hw := diff + 100
	return buildTiers(hw), true, diff, hw
}

// collectScanTargets 收集本轮打洞的全部候选地址。
func (e *Edge) collectScanTargets(instr *NatHoleInstruction) []*net.UDPAddr {
	var raw []*net.UDPAddr

	e.peersMu.RLock()
	var pLan, pAssisted []string
	var pUDPAddr *net.UDPAddr
	var pLastRecv int64
	if p := e.peers[instr.TargetMac]; p != nil {
		pLan = p.LanEndpoints
		pAssisted = p.AssistedEndpoints
		pUDPAddr = p.UDPAddr
		pLastRecv = p.lastRecvAt
	}
	e.peersMu.RUnlock()

	log.Printf("[collectScanTargets] target=%s p.Lan=%v p.Assisted=%v instr.TargetLan=%v instr.TargetAssisted=%v TargetPubSocket=%q",
		instr.TargetMac, pLan, pAssisted, instr.TargetLanEndpoints, instr.TargetAssistedEndpoints, instr.TargetPubSocket)

	// 1. 本机 peer 记录里的 LAN 地址
	for _, lan := range pLan {
		if addr := parseSockAddr(lan); addr != nil {
			raw = append(raw, addr)
		}
	}

	// 2. 观测 raddr
	if pUDPAddr != nil && pLastRecv > 0 {
		raw = append(raw, &net.UDPAddr{IP: pUDPAddr.IP, Port: pUDPAddr.Port})
	}

	// 3. 服务端下发的 assisted
	for _, ep := range instr.TargetAssistedEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			raw = append(raw, addr)
		}
	}

	// 4. 服务端下发的 LAN
	for _, ep := range instr.TargetLanEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			raw = append(raw, addr)
		}
	}

	// 去重
	seen := make(map[string]bool)
	var out []*net.UDPAddr
	for _, a := range raw {
		key := a.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, a)
	}
	log.Printf("[collectScanTargets] 去重后 %d 个候选: %v", len(out), out)
	return out
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

	assistedTargets := e.collectScanTargets(instr)
	log.Printf("[NAT-HOLE] 多目标扫描：LAN+raddr+assisted 共 %d 个候选", len(assistedTargets))

	if instr.Role == 0 && instr.SendDelayMs > 0 {
		time.Sleep(time.Duration(instr.SendDelayMs) * time.Millisecond)
	}

	probe := buildPunchProbe(e.GetVirtualIP())
	var attempts uint32

	tiers, sameStun, portDiff, halfWidth := e.chooseScanTiers(targetAddr)

	if sameStun && halfWidth < 0 {
		res.State = PunchStateFailed
		res.Detail = fmt.Sprintf("端口差过大 (%d)，放弃打洞", portDiff)
		log.Printf("[NAT-HOLE] ❌ 端口差=%d 过大（>5000），放弃打洞（直接走 TURN）", portDiff)

		natHoleActiveMu.Lock()
		failCounts[instr.TargetMac] = maxConsecutiveFails
		natHoleActiveMu.Unlock()

		return res
	}

	if sameStun && halfWidth > 0 {
		log.Printf("[NAT-HOLE] 模式：同 STUN IP，端口差=%d，精准扫描 targetPort±%d，分级 %v",
			portDiff, halfWidth, tiers)
	} else if sameStun {
		log.Printf("[NAT-HOLE] 模式：同 STUN IP 但端口差=%d（≥100），全端口扫描，分级 %v",
			portDiff, tiers)
	} else {
		log.Printf("[NAT-HOLE] 模式：不同 STUN IP，全端口扫描，分级 %v", tiers)
	}

	// 优先扫候选列表
	if len(assistedTargets) > 0 {
		log.Printf("[NAT-HOLE] 优先扫描 %d 个候选端点", len(assistedTargets))
		for _, t := range assistedTargets {
			if _, err := e.udpConn.WriteTo(probe, t); err == nil {
				attempts++
			}
		}

		select {
		case <-e.doneCh:
			res.State = PunchStateFailed
			res.Detail = "aborted: edge stopped"
			return res
		case <-time.After(300 * time.Millisecond):
		}

		if e.hasTrafficFromTarget(instr.TargetMac, startAt) {
			res.State = PunchStateSucceeded
			res.Attempts = attempts
			res.Detail = fmt.Sprintf("候选命中 (%d 个端点)", len(assistedTargets))
			e.recordP2PSuccess(instr, targetAddr)
			log.Printf("[NAT-HOLE] ✅ 成功 (candidates) role=%d target=%s attempts=%d",
				instr.Role, targetAddr.IP, attempts)
			e.resetFailCount(instr.TargetMac)
			return res
		}
		log.Printf("[NAT-HOLE] 候选未命中，进入 tier 扫描")
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

// recordP2PSuccess 打洞成功时更新 peer 地址。
//
// ★ 修复：LAN 优先保护
//
// targetAddr 来自 resolveTarget，通常是 peer.UDPAddr（已被保护），
// 但如果 resolveTarget 走到了 fallback 分支（peer.UDPAddr 为 nil），
// 就会返回指令里的 TargetPubSocket（公网）。
//
// 无条件覆盖会把 LAN 打成公网，之后 SendToPeer 走 CGNAT 回环（100~300ms）。
// 加 LAN 优先后，只有真正需要才切换。
func (e *Edge) recordP2PSuccess(instr *NatHoleInstruction, targetAddr *net.UDPAddr) {
	e.peersMu.Lock()
	if p, ok := e.peers[instr.TargetMac]; ok {
		// ★ LAN 优先保护
		newIsLAN := isPrivateIPv4(targetAddr.IP)
		oldIsLAN := p.UDPAddr != nil && isPrivateIPv4(p.UDPAddr.IP)
		now := time.Now().UnixMilli()

		shouldUpdate := false
		if p.UDPAddr == nil {
			shouldUpdate = true
		} else if !oldIsLAN && newIsLAN {
			shouldUpdate = true
		} else if oldIsLAN && !newIsLAN {
			// 旧 LAN → 新公网：只在 lastLANRecvAt 超过 300 秒时才切
			if p.lastLANRecvAt > 0 && now-p.lastLANRecvAt > 300000 {
				shouldUpdate = true
			}
		} else {
			// 同类型，端口变了才更新
			if p.UDPAddr.Port != targetAddr.Port {
				shouldUpdate = true
			}
		}

		if newIsLAN {
			p.lastLANRecvAt = now
		}

		if shouldUpdate {
			p.UDPAddr = targetAddr
		}
		p.lastRecvAt = now
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

// resolveTarget 决定这次打洞往哪个地址发包。
//
// ★ 对齐 PC：优先使用观测 raddr（已被证明可达）。
func (e *Edge) resolveTarget(instr *NatHoleInstruction) *net.UDPAddr {
	e.peersMu.RLock()
	if p := e.peers[instr.TargetMac]; p != nil && p.UDPAddr != nil && p.lastRecvAt > 0 {
		raddr := &net.UDPAddr{IP: p.UDPAddr.IP, Port: p.UDPAddr.Port}
		e.peersMu.RUnlock()
		log.Printf("[NAT-HOLE] 优先使用观测 raddr %s（已被证明可达）", raddr.String())
		return raddr
	}
	e.peersMu.RUnlock()

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

// buildPunchProbe 已移到 punch.go
