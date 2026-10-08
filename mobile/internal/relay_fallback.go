package internal

import (
	"log"
	"net"
	"sync"
	"time"
)

type ConnType string

const (
	ConnP2P     ConnType = "p2p"
	ConnTURN    ConnType = "turn"
	ConnRelay   ConnType = "relay"
	ConnUnknown ConnType = "unknown"
)

type RelayManager struct {
	mu         sync.RWMutex
	ws         *WSTransport
	turnClient *TURNClient
	edge       *Edge
	states     map[string]ConnType
}

func NewRelayManager(ws *WSTransport, turnClient *TURNClient, edge *Edge) *RelayManager {
	return &RelayManager{
		ws:         ws,
		turnClient: turnClient,
		edge:       edge,
		states:     make(map[string]ConnType),
	}
}

func (rm *RelayManager) MarkP2P(peerId string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] != ConnP2P {
		log.Printf("[连接] %s → P2P 直连", peerId)
		rm.states[peerId] = ConnP2P
	}
}

func (rm *RelayManager) MarkFallback(peerId string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		return
	}
	if rm.turnClient != nil && rm.turnClient.IsReady() {
		rm.states[peerId] = ConnTURN
		log.Printf("[连接] %s → TURN 中继", peerId)
		return
	}
	rm.states[peerId] = ConnRelay
	log.Printf("[连接] %s → WS 中继（TURN 未就绪）", peerId)
}

func (rm *RelayManager) UpgradeRelaysToTURN() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.turnClient == nil || !rm.turnClient.IsReady() {
		return
	}
	upgraded := 0
	for peerId, state := range rm.states {
		if state == ConnRelay {
			rm.states[peerId] = ConnTURN
			upgraded++
		}
	}
	if upgraded > 0 {
		log.Printf("[连接] TURN 就绪，升级 %d 个 WS 中继到 TURN", upgraded)
	}
}

func (rm *RelayManager) DowngradeToWS(peerId string, reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] != ConnRelay {
		log.Printf("[连接] %s → WS 中继 (%s)", peerId, reason)
		rm.states[peerId] = ConnRelay
	}
}

func (rm *RelayManager) ShouldRelay(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	s, ok := rm.states[peerId]
	if !ok {
		return true
	}
	return s == ConnTURN || s == ConnRelay
}

func (rm *RelayManager) GetState(peerId string) ConnType {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if s, ok := rm.states[peerId]; ok {
		return s
	}
	return ConnUnknown
}

// SendToPeer 三级降级：P2P → TURN → WS
func (rm *RelayManager) SendToPeer(peerId string, data []byte, target *PeerInfo) bool {
	state := rm.GetState(peerId)

	switch state {
	case ConnP2P:
		if target != nil && target.UDPAddr != nil {
			_, err := rm.edge.udpConn.WriteTo(data, target.UDPAddr)
			if err == nil {
				return true
			}
			log.Printf("[P2P] UDP 发送到 %s 失败: %v", peerId, err)
		}
		// 单播失败时不再 WS 双发——保持和 PC 端一致。
		// 上一版本在这里会 `rm.ws.SendBinary(data)` 兜底，
		// 结果每包走两条路径（P2P + WS），浪费带宽且让接收端收到重复。
		// 真正的降级由 MarkFallback 在几次失败后触发，不是每包兜底。
		return false

	case ConnTURN:
		if target != nil && target.TurnRelayAddr != "" && rm.turnClient != nil {
			relayAddr, err := net.ResolveUDPAddr("udp", target.TurnRelayAddr)
			if err == nil {
				if err := rm.turnClient.Send(data, relayAddr); err == nil {
					return true
				}
				log.Printf("[TURN] 发送到 %s 失败: %v", peerId, err)
			}
		}
		rm.DowngradeToWS(peerId, "send failed")
		return rm.ws.SendBinary(data) == nil

	case ConnRelay:
		return rm.ws.SendBinary(data) == nil
	}

	return false
}

// Report 定期上报连接状态。
//
// 通过 rm.edge.doneCh 感知 Edge 停止，避免 goroutine 泄漏
// （反复启停 VPN 时若旧 goroutine 不退出，每次会累积一个 + 每 10 秒一次无效网络调用）。
func (rm *RelayManager) Report(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rm.mu.RLock()
				conns := make(map[string]string)
				for peerId, t := range rm.states {
					conns[peerId] = string(t)
				}
				rm.mu.RUnlock()
				if len(conns) == 0 {
					continue
				}
				_ = rm.ws.Send(map[string]interface{}{
					"type":    "connection_status",
					"payload": map[string]interface{}{"connections": conns},
				})
			case <-rm.edge.doneCh:
				return
			}
		}
	}()
}
