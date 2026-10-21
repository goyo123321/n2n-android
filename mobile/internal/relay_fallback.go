package internal

import (
	"log"
	"net"
	"sync"
	"time"
)

type ConnType string

const (
	ConnUnknown      ConnType = "unknown"
	ConnP2PAvailable ConnType = "p2p-avail"
	ConnP2P          ConnType = "p2p"
	ConnTURN         ConnType = "turn"
	ConnRelay        ConnType = "relay"
)

// safeGo 定义在 edge.go 里。

type RelayManager struct {
	mu         sync.RWMutex
	ws         *WSTransport
	turnClient *TURNClient
	edge       *Edge
	states     map[string]ConnType
	failCounts map[string]int
}

func NewRelayManager(ws *WSTransport, turnClient *TURNClient, edge *Edge) *RelayManager {
	return &RelayManager{
		ws:         ws,
		turnClient: turnClient,
		edge:       edge,
		states:     make(map[string]ConnType),
		failCounts: make(map[string]int),
	}
}

func (rm *RelayManager) MarkP2PAvailable(peerID string) {
	rm.mu.Lock()
	cur := rm.states[peerID]
	if cur == ConnP2P || cur == ConnP2PAvailable {
		rm.mu.Unlock()
		return
	}
	rm.states[peerID] = ConnP2PAvailable
	rm.mu.Unlock()

	log.Printf("[连接] %s → P2PAvailable（打洞握手成功，等待数据帧验证）", peerID)
	rm.cancelFallbackTimer(peerID)

	if rm.edge != nil {
		rm.edge.cancelP2PAvailTimeout(peerID)
	}
}

func (rm *RelayManager) MarkP2P(peerId string) {
	rm.mu.Lock()
	if rm.states[peerId] == ConnP2P {
		rm.mu.Unlock()
		return
	}
	prev := rm.states[peerId]
	rm.states[peerId] = ConnP2P
	delete(rm.failCounts, peerId)
	rm.mu.Unlock()

	log.Printf("[连接] %s → P2P 直连（prev=%s，数据面已验证）", peerId, prev)
	rm.cancelFallbackTimer(peerId)

	if rm.edge != nil {
		rm.edge.cancelP2PAvailTimeout(peerId)
	}
}

func (rm *RelayManager) MarkFallback(peerId string) {
	rm.mu.Lock()

	if rm.states[peerId] == ConnP2P || rm.states[peerId] == ConnP2PAvailable {
		rm.mu.Unlock()
		rm.cancelFallbackTimer(peerId)
		log.Printf("[连接] %s 已 P2P/P2PAvailable，忽略 MarkFallback", peerId)
		return
	}
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		rm.mu.Unlock()
		rm.cancelFallbackTimer(peerId)
		return
	}
	rm.mu.Unlock()

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
			if idleMs < 10000 {
				log.Printf("[连接] %s 最近 %dms 收到过 UDP 包，跳过降级", peerId, idleMs)
				return
			}
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.states[peerId] == ConnP2P || rm.states[peerId] == ConnP2PAvailable {
		rm.cancelFallbackTimer(peerId)
		return
	}
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		rm.cancelFallbackTimer(peerId)
		return
	}

	if rm.turnClient != nil && rm.turnClient.IsReady() {
		rm.states[peerId] = ConnTURN
		log.Printf("[连接] %s → TURN 中继 (%s)", peerId, rm.turnClient.GetRelayAddr())
		rm.cancelFallbackTimer(peerId)
		return
	}

	rm.states[peerId] = ConnRelay
	log.Printf("[连接] %s → WS 中继 (TURN 未就绪，最后兜底)", peerId)
	rm.cancelFallbackTimer(peerId)
}

func (rm *RelayManager) ForceFallback(peerId string, reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.states[peerId] == ConnP2P {
		return
	}
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		return
	}

	if rm.turnClient != nil && rm.turnClient.IsReady() {
		rm.states[peerId] = ConnTURN
		log.Printf("[连接] %s → TURN 中继 (force: %s)", peerId, reason)
	} else {
		rm.states[peerId] = ConnRelay
		log.Printf("[连接] %s → WS 中继 (force: %s)", peerId, reason)
	}

	rm.cancelFallbackTimer(peerId)
	if rm.edge != nil {
		rm.edge.cancelP2PAvailTimeout(peerId)
	}
}

func (rm *RelayManager) ClearPeer(peerID string) {
	if peerID == "" {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.states, peerID)
	delete(rm.failCounts, peerID)
}

func (rm *RelayManager) ClearAll() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.states = make(map[string]ConnType)
	rm.failCounts = make(map[string]int)
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
	relayAddr := rm.turnClient.GetRelayAddr()
	upgraded := 0
	for peerId, state := range rm.states {
		if state == ConnRelay {
			rm.states[peerId] = ConnTURN
			upgraded++
			log.Printf("[连接] %s → TURN 中继 (延迟升级, %s)", peerId, relayAddr)
		}
	}
	if upgraded > 0 {
		log.Printf("[连接] TURN 就绪，升级 %d 个 WS 中继到 TURN", upgraded)
	}
}

func (rm *RelayManager) DowngradeToWS(peerId string, reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] == ConnP2P || rm.states[peerId] == ConnP2PAvailable {
		return
	}
	if rm.states[peerId] != ConnRelay {
		log.Printf("[连接] %s → WS 中继 (TURN 失败: %s)", peerId, reason)
		rm.states[peerId] = ConnRelay
	}
}

func (rm *RelayManager) MarkRelay(peerId string) {
	rm.MarkFallback(peerId)
}

func (rm *RelayManager) ShouldRelay(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	s, ok := rm.states[peerId]
	if !ok {
		return true
	}
	return s != ConnP2P && s != ConnP2PAvailable
}

func (rm *RelayManager) IsP2P(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.states[peerId] == ConnP2P
}

func (rm *RelayManager) IsP2PAvailable(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.states[peerId] == ConnP2PAvailable
}

func (rm *RelayManager) IsTURN(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.states[peerId] == ConnTURN
}

func (rm *RelayManager) IsWSRelay(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.states[peerId] == ConnRelay
}

func (rm *RelayManager) GetState(peerId string) ConnType {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if s, ok := rm.states[peerId]; ok {
		return s
	}
	return ConnUnknown
}

func (rm *RelayManager) SendViaRelay(data []byte) error {
	return rm.ws.SendBinary(data)
}

// SendToPeer 按当前状态发送数据。
// ★ ConnP2P 分支加连续失败计数，连续 5 次失败才降级。
// ★ Android 特有：udpConn 是 net.PacketConn，用 WriteTo。
func (rm *RelayManager) SendToPeer(peerId string, data []byte, target *PeerInfo) bool {
	state := rm.GetState(peerId)

	switch state {
	case ConnP2P, ConnP2PAvailable:
		if target == nil {
			rm.MarkFallback(peerId)
			return false
		}
		if rm.edge == nil || rm.edge.udpConn == nil {
			rm.MarkFallback(peerId)
			return false
		}

		sent := false

		if target.UDPAddr != nil &&
			!isPrivateIPv4(target.UDPAddr.IP) &&
			len(target.LanEndpoints) > 0 {
			for _, lan := range target.LanEndpoints {
				ra, err := net.ResolveUDPAddr("udp", lan)
				if err != nil || ra == nil || ra.IP == nil {
					continue
				}
				if _, err := rm.edge.udpConn.WriteTo(data, ra); err == nil {
					sent = true
					break
				}
			}
		}

		if !sent && target.UDPAddr != nil {
			if _, err := rm.edge.udpConn.WriteTo(data, target.UDPAddr); err == nil {
				sent = true
			} else {
				log.Printf("[P2P] UDP 发送到 %s 失败: %v", peerId, err)
			}
		}

		if sent {
			rm.mu.Lock()
			delete(rm.failCounts, peerId)
			rm.mu.Unlock()
			return true
		}

		rm.mu.Lock()
		rm.failCounts[peerId]++
		fc := rm.failCounts[peerId]
		rm.mu.Unlock()

		if fc < 5 {
			return false
		}

		rm.mu.Lock()
		delete(rm.failCounts, peerId)
		rm.mu.Unlock()
		log.Printf("[P2P] UDP 发送到 %s 连续失败 %d 次，降级", peerId, fc)
		rm.MarkFallback(peerId)
		return rm.ws.SendBinary(buildN2NWFrame(peerId, data)) == nil

	case ConnTURN:
		if target != nil && target.TurnRelayAddr != "" && rm.turnClient != nil {
			relayAddr, err := net.ResolveUDPAddr("udp", target.TurnRelayAddr)
			if err == nil {
				if sendErr := rm.turnClient.Send(data, relayAddr); sendErr == nil {
					return true
				} else {
					log.Printf("[TURN] 发送到 %s 失败（临时）: %v", peerId, sendErr)
					if rm.ws != nil {
						_ = rm.ws.SendBinary(buildN2NWFrame(peerId, data))
					}
					return false
				}
			} else {
				log.Printf("[TURN] 解析中继地址 %q 失败: %v", target.TurnRelayAddr, err)
			}
		}
		if rm.ws != nil {
			return rm.ws.SendBinary(buildN2NWFrame(peerId, data)) == nil
		}
		return false

	case ConnRelay:
		err := rm.ws.SendBinary(buildN2NWFrame(peerId, data))
		return err == nil
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
