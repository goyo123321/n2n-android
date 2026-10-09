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
// ★ 关键修复（补充）：
//   - P2P 已建立时不允许降级
//   - peer 最近 3 秒收到过对端 UDP 包时，跳过降级（时序问题）
func (rm *RelayManager) MarkFallback(peerId string) {
	rm.mu.Lock()

	// P2P 已建立 → 不可降级
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

	rm.mu.Unlock()

	// ★ 检查最近是否收到过对端 UDP 包
	if rm.edge != nil {
		rm.edge.peersMu.RLock()
		p, ok := rm.edge.peers[peerId]
		var lastRecv int64 = 0
		if ok {
			lastRecv = p.lastRecvAt
		}
		rm.edge.peersMu.RUnlock()

		if lastRecv > 0 {
			idleMs := time.Now().UnixMilli() - lastRecv
			if idleMs < 3000 {
				log.Printf("[连接] %s 最近 %dms 收到过 UDP 包，跳过降级", peerId, idleMs)
				return
			}
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 再次检查（防止 unlock 期间状态变化）
	if rm.states[peerId] == ConnP2P {
		rm.cancelFallbackTimer(peerId)
		return
	}
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		rm.cancelFallbackTimer(peerId)
		return
	}

	if rm.turnClient != nil && rm.turnClient.IsReady() {
		rm.states[peerId] = ConnTURN
		log.Printf("[连接] %s → TURN 中继", peerId)
		rm.cancelFallbackTimer(peerId)
		return
	}

	rm.states[peerId] = ConnRelay
	log.Printf("[连接] %s → WS 中继（TURN 未就绪，5s 内 TURN 就绪则自动升级）", peerId)

	rm.cancelFallbackTimer(peerId)

	safeGo("relay-upgrade-retry", func() {
		time.Sleep(5 * time.Second)
		rm.mu.Lock()
		defer rm.mu.Unlock()
		if rm.states[peerId] != ConnRelay {
			return
		}
		if rm.turnClient != nil && rm.turnClient.IsReady() {
			rm.states[peerId] = ConnTURN
			log.Printf("[连接] %s → TURN 中继（延迟升级）", peerId)
		}
	})
}

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
