package internal

import (
	"log"
	"net"
	"strconv"
	"time"
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

func (e *Edge) executeNatHole(instr *NatHoleInstruction) *PunchResult {
	startAt := time.Now().UnixMilli()
	res := &PunchResult{State: PunchStateInProgress, BehaviorIndex: instr.BehaviorIndex}

	targetAddr := e.resolveTarget(instr)
	if targetAddr == nil {
		res.State = PunchStateFailed
		res.Detail = "无法解析目标地址"
		return res
	}

	log.Printf("[NAT-HOLE] 开始 role=%d target=%s:%d rung=%d mode=%d",
		instr.Role, targetAddr.IP, targetAddr.Port, instr.BehaviorIndex, instr.Mode)

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

	rounds := 5
	if instr.TTL > 0 {
		rounds = instr.TTL * 2
		if rounds < 5 {
			rounds = 5
		}
		if rounds > 30 {
			rounds = 30
		}
	}

	for round := 0; round < rounds; round++ {
		for _, t := range targets {
			if _, err := e.udpConn.WriteToUDP(probe, t); err == nil {
				attempts++
			}
		}
		if e.hasPeerTrafficFromAny(candidateIPs, startAt) {
			res.State = PunchStateSucceeded
			res.Attempts = attempts
			res.Detail = "收到对端流量"
			e.peersMu.Lock()
			if p, ok := e.peers[instr.TargetMac]; ok {
				p.UDPAddr = targetAddr
				p.lastRecvAt = time.Now().UnixMilli()
			}
			e.peersMu.Unlock()
			log.Printf("[NAT-HOLE] ✅ 成功 attempts=%d", attempts)
			return res
		}
		time.Sleep(200 * time.Millisecond)
	}

	res.State = PunchStateFailed
	res.Attempts = attempts
	res.Detail = "无响应"
	log.Printf("[NAT-HOLE] ❌ 失败 attempts=%d", attempts)
	return res
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
	copy(buf[0:4], []byte{0x4e, 0x32, 0x4e, 0x50})
	if ip := net.ParseIP(virtualIP); ip != nil && ip.To4() != nil {
		copy(buf[4:8], ip.To4())
	}
	return buf
}

func (e *Edge) hasPeerTrafficFromAny(ips []net.IP, since int64) bool {
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
