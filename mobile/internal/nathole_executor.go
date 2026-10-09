package internal

import (
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

type NatHoleInstruction struct {
	Role                      int      `json:"role"`
	TTL                       int      `json:"ttl"`
	SendDelayMs               int      `json:"sendDelayMs"`
	PortsRangeFrom            uint32   `json:"portsRangeFrom"`
	PortsRangeTo              uint32   `json:"portsRangeTo"`
	TargetMac                 string   `json:"targetMac"`
	TargetVirtualIp           string   `json:"targetVirtualIp"`
	TargetPubSocket           string   `json:"targetPubSocket"`
	TargetAssistedEndpoints   []string `json:"targetAssistedEndpoints"`
	TargetLanEndpoints        []string `json:"targetLanEndpoints"` // ★ 新增
	SenderMac                 string   `json:"senderMac"`
	SenderP2PEndpoint         string   `json:"senderP2pEndpoint"`
	SenderPubSocket           string   `json:"senderPubSocket"`
	SenderNatType             string   `json:"senderNatType"`
	SenderBehavior            string   `json:"senderBehavior"`
	SenderAssistedEndpoints   []string `json:"senderAssistedEndpoints"`
	ReceiverMac               string   `json:"receiverMac"`
	ReceiverP2PEndpoint       string   `json:"receiverP2pEndpoint"`
	ReceiverPubSocket         string   `json:"receiverPubSocket"`
	ReceiverNatType           string   `json:"receiverNatType"`
	ReceiverAssistedEndpoints []string `json:"receiverAssistedEndpoints"`
	PortsDifference           int      `json:"portsDifference"`
	RegularPortsChange        bool     `json:"regularPortsChange"`
	Mode                      int      `json:"mode"`
	BehaviorIndex             int      `json:"behaviorIndex"`
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

var probePrefix = []byte{0x4E, 0x32, 0x4E, 0x50} // "N2NP"

// 同一时刻只允许一个 executeNatHole 在跑。
var (
	natHoleActiveMu sync.Mutex
	natHoleActive   map[string]bool
)

func init() {
	natHoleActive = make(map[string]bool)
}

func (e *Edge) executeNatHole(instr *NatHoleInstruction) *PunchResult {
	// 已 P2P 就跳过
	if e.relayMgr != nil && e.relayMgr.GetState(instr.TargetMac) == ConnP2P {
		log.Printf("[NAT-HOLE] 跳过指令 target=%s（已 P2P）", instr.TargetMac)
		return nil
	}

	// 同一 target 已有执行中的指令，返回 nil
	natHoleActiveMu.Lock()
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

	// 立即上报 InProgress
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

	// TTL 是 socket 的 IP TTL，不是轮次数
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

	// === 构建目标地址列表：LAN 优先 ===
	var targets []*net.UDPAddr
	var candidateIPs []net.IP

	// 1. ★ LAN endpoints 优先
	for _, ep := range instr.TargetLanEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			targets = append(targets, addr)
			candidateIPs = append(candidateIPs, addr.IP)
		}
	}
	if len(targets) > 0 {
		log.Printf("[NAT-HOLE] LAN 候选 %d 个（优先尝试）", len(targets))
	}

	// 2. assisted endpoints
	for _, ep := range instr.TargetAssistedEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			targets = append(targets, addr)
			candidateIPs = append(candidateIPs, addr.IP)
		}
	}

	// 3. 端口扫描范围或公网单点
	if instr.PortsRangeFrom > 0 && instr.PortsRangeTo >= instr.PortsRangeFrom {
		count := int(instr.PortsRangeTo - instr.PortsRangeFrom + 1)
		if count > 100 {
			count = 100
		}
		for i := 0; i < count; i++ {
			targets = append(targets, &net.UDPAddr{
				IP:   targetAddr.IP,
				Port: int(instr.PortsRangeFrom) + i,
			})
		}
	} else {
		targets = append(targets, targetAddr)
	}
	candidateIPs = append(candidateIPs, targetAddr.IP)

	if instr.Role == 0 && instr.SendDelayMs > 0 {
		time.Sleep(time.Duration(instr.SendDelayMs) * time.Millisecond)
	}

	probe := buildPunchProbe(e.virtualIP)
	var attempts uint32

	const rounds = 5
	const roundInterval = 200 * time.Millisecond

	for round := 0; round < rounds; round++ {
		for _, t := range targets {
			if _, err := e.udpConn.WriteTo(probe, t); err == nil {
				attempts++
			}
		}
		if e.hasRealTrafficFromAny(candidateIPs, startAt) {
			res.State = PunchStateSucceeded
			res.Attempts = attempts
			res.Detail = "收到对端真实数据帧"

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

			log.Printf(
				"[NAT-HOLE] ✅ 成功 role=%d target=%s attempts=%d",
				instr.Role, targetAddr.IP, attempts,
			)
			return res
		}
		time.Sleep(roundInterval)
	}

	res.State = PunchStateFailed
	res.Attempts = attempts
	res.Detail = "无响应"
	log.Printf(
		"[NAT-HOLE] ❌ 失败 role=%d target=%s attempts=%d",
		instr.Role, targetAddr.IP, attempts,
	)
	return res
}

// 向服务端上报 InProgress
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

func (e *Edge) hasRealTrafficFromAny(ips []net.IP, since int64) bool {
	if len(ips) == 0 {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	for _, p := range e.peers {
		if p.UDPAddr == nil || p.lastRecvAt < since {
			continue
		}
		if !p.hasRealData {
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
