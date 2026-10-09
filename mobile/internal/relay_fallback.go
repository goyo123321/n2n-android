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

// MarkP2P 升级到 P2P。
//
// ★ P2P 是最高优先级，一旦建立不被任何降级覆盖。
//   打印 prev 状态便于追踪状态迁移。
func (rm *RelayManager) MarkP2P(peerId string) {
	rm.mu.Lock()
	if rm.states[peerId] == ConnP2P {
		rm.mu.Unlock()
		return
	}
	prev := rm.states[peerId]
	rm.states[peerId] = ConnP2P
	rm.mu.Unlock()

	log.Printf("[连接] %s → P2P 直连（prev=%s）", peerId, prev)
	rm.cancelFallbackTimer(peerId)
}

// MarkFallback P2P 失败时降级。
//
// ★ 关键修复：P2P 已建立时不允许降级。
//   多个 rung 的 executeNatHole 并发跑时，rung A 成功升级 P2P 后，
//   rung B 才失败调 MarkFallback，会把 P2P 覆盖成 TURN，造成状态抖动。
//
// 其他逻辑不变：TURN 就绪 → TURN；未就绪 → 暂时 WS + 5s 延迟升级。
func (rm *RelayManager) MarkFallback(peerId string) {
	rm.mu.Lock()

	// ★ P2P 已建立 → 不可降级
	if rm.states[peerId] == ConnP2P {
		rm.mu.Unlock()
		rm.cancelFallbackTimer(peerId)
		log.Printf("[连接] %s 已 P2P，忽略 MarkFallback", peerId)
		return
	}

	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		rm.mu.Unlock()
		rm.cancelFallbackTimer(peerId)
		return
	}

	if rm.turnClient != nil && rm.turnClient.IsReady() {
		rm.states[peerId] = ConnTURN
		rm.mu.Unlock()
		log.Printf("[连接] %s → TURN 中继", peerId)
		rm.cancelFallbackTimer(peerId)
		return
	}

	rm.states[peerId] = ConnRelay
	rm.mu.Unlock()
	log.Printf("[连接] %s → WS 中继（TURN 未就绪，5s 内 TURN 就绪则自动升级）", peerId)

	rm.cancelFallbackTimer(peerId)

	// 延迟升级：5s 后如果 TURN 已就绪且仍是 WS，从 WS 升级到 TURN
	safeGo("relay-upgrade-retry", func() {
		time.Sleep(5 * time.Second)
		rm.mu.Lock()
		defer rm.mu.Unlock()
		// ★ 再次检查：5s 内可能已经被升级到 P2P 或 TURN
		if rm.states[peerId] != ConnRelay {
			return
		}
		if rm.turnClient != nil && rm.turnClient.IsReady() {
			rm.states[peerId] = ConnTURN
			log.Printf("[连接] %s → TURN 中继（延迟升级）", peerId)
		}
	})
}

// cancelFallbackTimer 取消 edge 上该 peer 的超时降级定时器。
func (rm *RelayManager) cancelFallbackTimer(peerID string) {
	if rm.edge == nil {
		return
	}
	rm.edge.cancelFallbackTimer(peerID)
}

func (rm *RelayManager) UpgradeRelaysToTURN() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.turnClient == nil || !rm.turnClient.IsReady() {
		return
	}
	upgraded := 0
	for peerId, state := range rm.states {
		// ★ 只升级 WS 到 TURN，不动 P2P
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
	// ★ P2P 不降级
	if rm.states[peerId] == ConnP2P {
		return
	}
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

func (rm *RelayManager) SendToPeer(peerId string, data []byte, target *PeerInfo) bool {
	state := rm.GetState(peerId)

	switch state {
	case ConnP2P:
		if target != nil && target.UDPAddr != nil && rm.edge != nil && rm.edge.udpConn != nil {
			_, err := rm.edge.udpConn.WriteTo(data, target.UDPAddr)
			if err == nil {
				return true
			}
			log.Printf("[P2P] UDP 发送到 %s 失败: %v", peerId, err)
		}
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
		if rm.ws == nil {
			return false
		}
		return rm.ws.SendBinary(data) == nil

	case ConnRelay:
		if rm.ws == nil {
			return false
		}
		return rm.ws.SendBinary(data) == nil
	}

	return false
}

func (rm *RelayManager) Report(interval time.Duration) {
	if rm.edge == nil {
		return
	}
	safeGo("relay-report", func() {
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
				if rm.ws == nil {
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
	})
}
