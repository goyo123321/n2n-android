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
	ConnLAN          ConnType = "lan"
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

	// ★ 新增：peer-via-ws 震荡保护
	//   5 秒内累计 ≥3 个 WS 帧才降级到 P2PAvailable
	//   避免 B 的 LAN probe 和 TURN 失败兜底 WS 导致 A 震荡
	wsFrameCounts map[string]int
	wsFirstAt     map[string]int64
}

func NewRelayManager(ws *WSTransport, turnClient *TURNClient, edge *Edge) *RelayManager {
	return &RelayManager{
		ws:            ws,
		turnClient:    turnClient,
		edge:          edge,
		states:        make(map[string]ConnType),
		failCounts:    make(map[string]int),
		wsFrameCounts: make(map[string]int),
		wsFirstAt:     make(map[string]int64),
	}
}

// MarkP2PAvailable 标记打洞握手成功（等待数据帧验证）。
func (rm *RelayManager) MarkP2PAvailable(peerID string) {
	rm.mu.Lock()
	cur := rm.states[peerID]
	if cur == ConnLAN || cur == ConnP2P || cur == ConnP2PAvailable {
		rm.mu.Unlock()
		return
	}
	rm.states[peerID] = ConnP2PAvailable
	rm.mu.Unlock()

	log.Printf("[连接] %s → P2PAvailable（打洞握手成功，等待数据帧验证）", peerID)
	rm.cancelFallbackTimer(peerID)

	if rm.edge != nil {
		rm.edge.cancelP2PAvailTimeout(peerID)
		rm.edge.reportP2PStatus(peerID, 2)
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
		rm.edge.reportP2PStatus(peerId, 3)
	}
}

// MarkLAN 标记 LAN 直连已验证。
func (rm *RelayManager) MarkLAN(peerId string) {
	rm.mu.Lock()
	prev := rm.states[peerId]
	if prev == ConnLAN {
		rm.mu.Unlock()
		return
	}
	rm.states[peerId] = ConnLAN
	delete(rm.failCounts, peerId)
	rm.mu.Unlock()

	log.Printf("[连接] %s → LAN 直连（prev=%s，数据面已验证）", peerId, prev)
	rm.cancelFallbackTimer(peerId)

	if rm.edge != nil {
		rm.edge.cancelP2PAvailTimeout(peerId)
		rm.edge.reportP2PStatus(peerId, 3)
	}
}

// DemoteLANToP2P LAN 60s 无流量时降级到公网 P2P。
func (rm *RelayManager) DemoteLANToP2P(peerId, reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] != ConnLAN {
		return
	}
	rm.states[peerId] = ConnP2P
	log.Printf("[连接] %s LAN → P2P（%s）", peerId, reason)
}

// MarkFallback 从 Unknown 状态降级到 TURN/WS。
func (rm *RelayManager) MarkFallback(peerId string) {
	rm.mu.Lock()

	if rm.states[peerId] == ConnLAN ||
		rm.states[peerId] == ConnP2P ||
		rm.states[peerId] == ConnP2PAvailable {
		rm.mu.Unlock()
		rm.cancelFallbackTimer(peerId)
		log.Printf("[连接] %s 已 LAN/P2P/P2PAvailable，忽略 MarkFallback", peerId)
		return
	}
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		rm.mu.Unlock()
		rm.cancelFallbackTimer(peerId)
		return
	}
	rm.mu.Unlock()

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.states[peerId] == ConnLAN ||
		rm.states[peerId] == ConnP2P ||
		rm.states[peerId] == ConnP2PAvailable {
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

// ForceFallback 强制降级。
func (rm *RelayManager) ForceFallback(peerId string, reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		return
	}

	if rm.states[peerId] == ConnLAN {
		if reason != "peer-via-turn" &&
			reason != "peer-via-ws" &&
			reason != "no-traffic-30s" &&
			reason != "network-changed" &&
			reason != "lan-endpoints-cleared" &&
			reason != "peer-uses-turn" {
			return
		}
		log.Printf("[连接] %s 从 LAN 降级 (force: %s)", peerId, reason)
	}

	if rm.states[peerId] == ConnP2P {
		if reason != "no-traffic-30s" &&
			reason != "p2p-avail-timeout" &&
			reason != "peer-via-turn" &&
			reason != "peer-via-ws" &&
			reason != "peer-uses-turn" {
			return
		}
		log.Printf("[连接] %s 从 P2P 降级 (force: %s)", peerId, reason)
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
		rm.edge.reportP2PStatus(peerId, 0)

		if rm.ws != nil {
			conns := make(map[string]string)
			for id, t := range rm.states {
				conns[id] = string(t)
			}
			if len(conns) > 0 {
				_ = rm.ws.Send(map[string]interface{}{
					"type":    "connection_status",
					"payload": map[string]interface{}{"connections": conns},
				})
			}
		}
	}
}

// ============================================================
// ★ 新增：RecordWSFrame
// ============================================================
//
// peer-via-ws 震荡保护：
//   5 秒内累计 ≥3 个 WS 帧才降级到 P2PAvailable
//
// 场景：B 的 TURN 失败走 WS 兜底发数据 → A 收到 WS 帧降级 TURN
//   但 B 的 sendP2PIdleKeepalive 每 2s 发 LAN probe → A 收到 Punch 升 LAN
//   → 反复震荡
//
// 修复：5 秒内 ≥3 帧才降级，避免单个 WS 帧就触发
func (rm *RelayManager) RecordWSFrame(peerID string) {
	if peerID == "" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	now := time.Now().UnixMilli()
	first, ok := rm.wsFirstAt[peerID]
	if !ok || now-first > 5000 {
		rm.wsFirstAt[peerID] = now
		rm.wsFrameCounts[peerID] = 0
	}
	rm.wsFrameCounts[peerID]++

	if rm.wsFrameCounts[peerID] < 3 {
		log.Printf("[连接] %s WS 帧计数 %d/3（%.1fs 窗口）",
			peerID, rm.wsFrameCounts[peerID], float64(now-first)/1000.0)
		return
	}

	// ≥3 帧：降级
	rm.wsFrameCounts[peerID] = 0
	rm.wsFirstAt[peerID] = 0

	cur := rm.states[peerID]
	if cur == ConnLAN || cur == ConnP2P || cur == ConnP2PAvailable {
		// 降级到 ConnP2PAvailable 而不是直接 TURN
		// 让客户端继续尝试 P2P，60s 超时后才真正降级
		rm.states[peerID] = ConnP2PAvailable
		log.Printf("[连接] %s %s → P2PAvailable（peer-via-ws ×3）", peerID, cur)
		if rm.edge != nil {
			rm.edge.cancelFallbackTimer(peerID)
			rm.edge.reportP2PStatus(peerID, 2)
			rm.edge.scheduleP2PAvailTimeout(peerID)
		}
	}
}

// ============================================================
// ★ 修复：TURN 不可用时直接走 WS
// ============================================================
//
// 场景：TURN over TCP 连接断开（broken pipe）后：
//   - turnClient.IsReady() 可能仍返回 true（懒检测）
//   - 或 TurnRelayAddr 仍在但实际不可达
//   → SendToPeer(ConnTURN) 反复发送失败
//   → 5 次后降级 WS
//
// 修复：每次发送前检查 turnClient.IsReady()，不可用直接走 WS
func (rm *RelayManager) SendToPeer(peerId string, data []byte, target *PeerInfo) bool {
	state := rm.GetState(peerId)

	switch state {
	case ConnLAN:
		if target == nil || rm.edge == nil || rm.edge.udpConn == nil {
			rm.MarkFallback(peerId)
			return false
		}

		if len(target.LanEndpoints) == 0 && (target.UDPAddr == nil || isPrivateIPv4(target.UDPAddr.IP)) {
			log.Printf("[连接] %s LanEndpoints 为空，且 UDPAddr 无公网地址，降级", peerId)
			rm.mu.Lock()
			wasLAN := rm.states[peerId] == ConnLAN
			if wasLAN {
				rm.states[peerId] = ConnP2PAvailable
			}
			rm.mu.Unlock()

			if wasLAN && rm.edge != nil {
				rm.edge.cancelFallbackTimer(peerId)
				rm.edge.reportP2PStatus(peerId, 2)
				rm.edge.scheduleP2PAvailTimeout(peerId)
			}
			return rm.ws.SendBinary(buildN2NWFrame(peerId, data)) == nil
		}

		sent := false
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
		if !sent && target.UDPAddr != nil && isPrivateIPv4(target.UDPAddr.IP) {
			if _, err := rm.edge.udpConn.WriteTo(data, target.UDPAddr); err == nil {
				sent = true
			}
		}
		if sent {
			return true
		}

		rm.mu.Lock()
		wasLAN := rm.states[peerId] == ConnLAN
		if wasLAN {
			rm.states[peerId] = ConnP2PAvailable
		}
		rm.mu.Unlock()

		if wasLAN {
			log.Printf("[连接] %s LAN 发送失败，降级到 P2PAvailable（等待验证）", peerId)
			if rm.edge != nil {
				rm.edge.cancelFallbackTimer(peerId)
				rm.edge.reportP2PStatus(peerId, 2)
				rm.edge.scheduleP2PAvailTimeout(peerId)
			}
		}

		if target.UDPAddr != nil && !isPrivateIPv4(target.UDPAddr.IP) {
			if _, err := rm.edge.udpConn.WriteTo(data, target.UDPAddr); err == nil {
				return true
			}
		}
		return rm.ws.SendBinary(buildN2NWFrame(peerId, data)) == nil

	case ConnP2P, ConnP2PAvailable:
		if target == nil {
			rm.MarkFallback(peerId)
			return false
		}
		if rm.edge == nil || rm.edge.udpConn == nil {
			rm.MarkFallback(peerId)
			return false
		}

		selfOnLAN := rm.edge.selfIsOnTargetLAN(target)
		sent := false

		if selfOnLAN && len(target.LanEndpoints) > 0 {
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
			skip := false
			if isPrivateIPv4(target.UDPAddr.IP) && !selfOnLAN {
				skip = true
				log.Printf("[P2P] %s UDPAddr=%s 是私网且本机不在该 LAN，跳过（避免黑洞）",
					peerId, target.UDPAddr)
			}
			if !skip {
				if _, err := rm.edge.udpConn.WriteTo(data, target.UDPAddr); err == nil {
					sent = true
				} else {
					log.Printf("[P2P] UDP 发送到 %s 失败: %v", peerId, err)
				}
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
		// ★ 修复：每次发送前检查 TURN 是否真的就绪
		//   TURN over TCP 断开后 IsReady() 可能仍返回 true
		//   → 直接走 WS 兜底，避免反复尝试失败的 TURN
		if rm.turnClient == nil || !rm.turnClient.IsReady() {
			log.Printf("[TURN] %s TURN 未就绪（可能已断开），直接走 WS 兜底", peerId)
			rm.mu.Lock()
			rm.states[peerId] = ConnRelay
			rm.mu.Unlock()
			if rm.ws != nil {
				return rm.ws.SendBinary(buildN2NWFrame(peerId, data)) == nil
			}
			return false
		}

		if target != nil && target.TurnRelayAddr != "" && rm.turnClient != nil {
			relayAddr, err := net.ResolveUDPAddr("udp", target.TurnRelayAddr)
			if err == nil {
				if sendErr := rm.turnClient.Send(data, relayAddr); sendErr == nil {
					rm.mu.Lock()
					delete(rm.failCounts, peerId)
					rm.mu.Unlock()
					return true
				} else {
					log.Printf("[TURN] 发送到 %s 失败（临时）: %v", peerId, sendErr)
					rm.mu.Lock()
					rm.failCounts[peerId]++
					fc := rm.failCounts[peerId]
					rm.mu.Unlock()
					if fc >= 3 {
						log.Printf("[TURN] %s 连续失败 %d 次，清空 relay 地址等重新上报", peerId, fc)
						rm.mu.Lock()
						delete(rm.failCounts, peerId)
						rm.mu.Unlock()
						if rm.edge != nil {
							rm.edge.peersMu.Lock()
							if p := rm.edge.peers[peerId]; p != nil {
								oldRelay := p.TurnRelayAddr
								p.TurnRelayAddr = ""
								log.Printf("[TURN] 清空 %s 的旧 relay 地址 %q", peerId, oldRelay)
							}
							rm.edge.peersMu.Unlock()
						}
					}
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

// ClearPeer 删除 peer 的本地状态。
func (rm *RelayManager) ClearPeer(peerID string) {
	if peerID == "" {
		return
	}

	rm.mu.Lock()
	delete(rm.states, peerID)
	delete(rm.failCounts, peerID)
	delete(rm.wsFrameCounts, peerID)   // ★ 清理震荡保护状态
	delete(rm.wsFirstAt, peerID)

	conns := make(map[string]string)
	for id, t := range rm.states {
		conns[id] = string(t)
	}
	rm.mu.Unlock()

	if rm.ws != nil {
		_ = rm.ws.Send(map[string]interface{}{
			"type":    "connection_status",
			"payload": map[string]interface{}{"connections": conns},
		})
	}
}

// ClearAll 清空所有本地状态。
func (rm *RelayManager) ClearAll() {
	rm.mu.Lock()
	rm.states = make(map[string]ConnType)
	rm.failCounts = make(map[string]int)
	rm.wsFrameCounts = make(map[string]int)   // ★ 清理震荡保护状态
	rm.wsFirstAt = make(map[string]int64)
	rm.mu.Unlock()

	if rm.ws != nil {
		_ = rm.ws.Send(map[string]interface{}{
			"type":    "connection_status",
			"payload": map[string]interface{}{"connections": map[string]string{}},
		})
	}
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
	if rm.states[peerId] == ConnLAN ||
		rm.states[peerId] == ConnP2P ||
		rm.states[peerId] == ConnP2PAvailable {
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
	return s != ConnLAN && s != ConnP2P && s != ConnP2PAvailable
}

func (rm *RelayManager) IsP2P(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.states[peerId] == ConnP2P
}

func (rm *RelayManager) IsLAN(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.states[peerId] == ConnLAN
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

// ResetP2PStates 重置所有 P2P / LAN / TURN 状态，保留 WS。
func (rm *RelayManager) ResetP2PStates(reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for peerID, state := range rm.states {
		if state == ConnLAN || state == ConnP2P || state == ConnP2PAvailable || state == ConnTURN {
			rm.states[peerID] = ConnUnknown
			log.Printf("[连接] %s %s → Unknown（%s）", peerID, state, reason)
		}
	}
	// 清空震荡保护状态
	rm.wsFrameCounts = make(map[string]int)
	rm.wsFirstAt = make(map[string]int64)
}
