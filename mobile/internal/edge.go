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

// probePrefix 是打洞探测包的前 4 字节标记 "N2NP"。
// 收到该前缀的 UDP 包只更新"最近对端活动时间"，不触发 P2P 升级。
var probePrefix = []byte{0x4E, 0x32, 0x4E, 0x50}

// 同一时刻只允许一个 executeNatHole 在跑。
//
// 原因：TTL 是 socket 级别的选项（ipv4.PacketConn.SetTTL），多个打洞
// 并发执行时各自的 SetTTL 会互相覆盖，导致部分轮次用错 TTL。串行化
// 会把这种竞争消掉。打洞不是热路径，串行化代价可以忽略。
//
// activeTargets 用于跳过重复指令：服务端在早期版本里会重复下发同一
// target 的指令，如果每次都起一个 goroutine，多个 executeNatHole 会
// 同时向同一目标发包。检测到同一 target 已在执行时直接跳过新指令。
var (
	natHoleActiveMu sync.Mutex
	natHoleActive   map[string]bool
)

func init() {
	natHoleActive = make(map[string]bool)
}

func (e *Edge) executeNatHole(instr *NatHoleInstruction) *PunchResult {
	// ★ 已 P2P 就跳过。
	//
	// 场景 1：LAN 直连 —— registerPeer 检测到同网段就直接 MarkP2P，
	//   服务端不知道这件事还会继续下发打洞指令。
	// 场景 2：上次打洞已成功 —— 服务端 punchState 可能已经清理，
	//   或者报文的 in-flight 窗口过期了，又下发一条。
	//
	// 无论哪种情况，直接跳过。返回 nil 让上层不上报，
	// 避免污染服务端的 failCounts 和 analyzer 分数。
	if e.relayMgr != nil && e.relayMgr.GetState(instr.TargetMac) == ConnP2P {
		log.Printf("[NAT-HOLE] 跳过指令 target=%s（已 P2P）", instr.TargetMac)
		return nil
	}

	// 同一 target 已有执行中的指令，返回 nil。
	//
	// 返回 nil 而不是 Failed：原指令还在执行，它会发真实结果。
	// 如果这里返回 Failed，服务端会多记一次假失败，污染 failCounts
	// 和 analyzer 分数。调用方 runNatHole 看到 nil 时跳过上报。
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

	// ★ 立即上报 InProgress。
	//
	// 服务端的 in-flight 窗口默认 10s。ladder 的 rung 7/9 带
	// sendDelayMs=10000，客户端会 sleep 10s 后才发第一个探测包——
	// 恰好卡在窗口边界上。不主动上报的话，服务端会在客户端开始
	// 打洞前就误判为"没有回应"并重新派发指令，导致：
	//   1. 服务端记录一次假失败
	//   2. failCounts 增加，下次 backoff 更久
	//   3. analyzer 错误惩罚该 rung
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

	// ★ TTL 是 socket 的 IP TTL，不是轮次数。
	//
	// ladder 里的 ttl 是 socket 的 IP TTL，设置 TTL 让探测包在特定跳数
	// 后消亡：
	//   TTL=7  → 走 7 跳即死，用于让 NAT 在近处分配映射（短路径）
	//   TTL=4  → 更短
	//   ttl=0  → 不改 TTL，全路径发送（长路径唯一能用的档位）
	//
	// 早期版本把 ttl 当循环次数，所有梯级的探测包行为几乎一样，
	// 整个 ladder 失去意义。
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
		"[NAT-HOLE] 开始打洞 role=%d target=%s:%d rung=%d mode=%d ttl=%d assisted=%d",
		instr.Role, targetAddr.IP, targetAddr.Port,
		instr.BehaviorIndex, instr.Mode, instr.TTL,
		len(instr.TargetAssistedEndpoints),
	)

	// === 构建目标地址列表 ===
	var targets []*net.UDPAddr
	var candidateIPs []net.IP

	for _, ep := range instr.TargetAssistedEndpoints {
		if addr := parseSockAddr(ep); addr != nil {
			targets = append(targets, addr)
			candidateIPs = append(candidateIPs, addr.IP)
		}
	}

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

	// 固定 5 轮，每轮 200ms。TTL 已经由 ipv4.PacketConn 处理。
	// 不再把 ttl 当轮次数。
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

// ★ 向服务端上报 InProgress。
//
// 放在 executeNatHole 开头而不是中途，是因为服务端的 in-flight 窗口
// 是从"派发时刻"开始计时的，而客户端从"收到指令"到"发出第一个探测包"
// 之间可能有 sendDelayMs（最长 10s）的延迟。只有客户端一收到就上报，
// 才能让服务端的窗口跟着延迟重新计时。
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

// ★ 判断候选 IP 中是否有过"真实数据帧"到达。
//
// 探测包不算——它是打洞本身产生的，只证明对方在打洞，
// 不证明数据平面通了。真实数据帧才是 P2P 成功的证据。
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
