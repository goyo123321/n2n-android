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

// ★ 同一 target 连续失败 N 次后，跳过后续指令
const maxConsecutiveFails = 5

var probePrefix = []byte{0x4E, 0x32, 0x4E, 0x50} // "N2NP"

var (
	natHoleActiveMu sync.Mutex
	natHoleActive   map[string]bool
	failCounts      map[string]int
)

func init() {
	natHoleActive = make(map[string]bool)
	failCounts = make(map[string]int)
}

// scanTiers 端口扫描分级：从窄到宽，逐级放大。
var scanTiers = []int{3, 10, 20, 30, 60, 100}

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
		"[NAT-HOLE] 开始打洞 role=%d target=%s:%d rung=%d mode=%d ttl=%d assisted=%d lan=%d",
		instr.Role, targetAddr.IP, targetAddr.Port,
		instr.BehaviorIndex, instr.Mode, instr.TTL,
		len(instr.TargetAssistedEndpoints),
		len(instr.TargetLanEndpoints),
	)

	var lanTargets []*net.UDPAddr
	var lanIPs []net.IP

	for _, ep := range instr.TargetLanEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			lanTargets = append(lanTargets, addr)
			lanIPs = append(lanIPs, addr.IP)
		}
	}

	var assistedTargets []*net.UDPAddr
	var publicIPs []net.IP
	publicIPs = append(publicIPs, targetAddr.IP)
	for _, ep := range instr.TargetAssistedEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			assistedTargets = append(assistedTargets, addr)
			publicIPs = append(publicIPs, addr.IP)
		}
	}

	if len(lanTargets) > 0 {
		log.Printf("[NAT-HOLE] LAN 候选 %d 个（阶段 1）", len(lanTargets))
	}

	if instr.Role == 0 && instr.SendDelayMs > 0 {
		time.Sleep(time.Duration(instr.SendDelayMs) * time.Millisecond)
	}

	probe := buildPunchProbe(e.virtualIP)
	var attempts uint32

	if len(lanTargets) > 0 {
		for i := 0; i < 3; i++ {
			for _, t := range lanTargets {
				if _, err := e.udpConn.WriteTo(probe, t); err == nil {
					attempts++
				}
			}
			if e.hasTrafficFromAny(lanIPs, startAt) {
				res.State = PunchStateSucceeded
				res.Attempts = attempts
				res.Detail = "LAN 直连成功"
				e.recordP2PSuccess(instr, targetAddr)
				log.Printf("[NAT-HOLE] ✅ 成功 (LAN) role=%d target=%s attempts=%d",
					instr.Role, targetAddr.IP, attempts)
				e.resetFailCount(instr.TargetMac)
				return res
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	if targetAddr != nil {
		lastHalf := 0
		receiverPort := targetAddr.Port

		log.Printf("[NAT-HOLE] 阶段 2：分阶段扫描，目标 %s:%d，分级 %v",
			targetAddr.IP, receiverPort, scanTiers)

		for _, half := range scanTiers {
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

			const tierRounds = 3
			const tierInterval = 100 * time.Millisecond

			success := false
			for r := 0; r < tierRounds; r++ {
				for _, t := range tierTargets {
					if _, err := e.udpConn.WriteTo(probe, t); err == nil {
						attempts++
					}
				}
				if e.hasTrafficFromAny(publicIPs, startAt) {
					success = true
					break
				}
				time.Sleep(tierInterval)
			}

			if success {
				res.State = PunchStateSucceeded
				res.Attempts = attempts
				res.Detail = fmt.Sprintf("公网端口扫描成功 (tier=±%d)", half)
				e.recordP2PSuccess(instr, targetAddr)
				log.Printf("[NAT-HOLE] ✅ 成功 (公网) role=%d target=%s attempts=%d tier=±%d",
					instr.Role, targetAddr.IP, attempts, half)
				e.resetFailCount(instr.TargetMac)
				return res
			}

			log.Printf("[NAT-HOLE] 阶段 2 tier=±%d 未命中（本轮 %d 个），扩大范围（累计 attempts=%d）",
				half, len(tierTargets), attempts)
			lastHalf = half
		}
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

// ★ hasTrafficFromAny 定义在 edge.go（两个文件都定义会冲突）
