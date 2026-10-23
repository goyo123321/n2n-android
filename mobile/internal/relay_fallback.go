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

// MarkP2PAvailable 标记打洞握手成功（等待数据帧验证）。
//
// ★ 修复：新增 reportP2PStatus(peerID, 2)。
//   notePeerCommon 的 Punch/ACK 路径也走这个函数，旧版不上报 p2pStatus=2，
//   导致服务端 punchState.aP2PStatus 一直是 0，可能持续重发指令。
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
		rm.edge.reportP2PStatus(peerID, 2) // ★ 新增
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
//
// LAN 和公网 P2P 分开：
//   - LAN 不需要 2s 保活（不对抗 WiFi 省电）
//   - LAN 60s 无流量才降级（而非 30s）
//   - LAN 不受 force_fallback 影响（服务端已跳过 LAN peer）
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

// DemoteLANToP2P LAN 60s 无流量时降级到公网 P2P（不是 TURN）。
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
//
// ★ ConnLAN / ConnP2P / ConnP2PAvailable 视为「已建立」，不降级。
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
//
// ★ ConnLAN 白名单：peer-via-turn / peer-via-ws / no-traffic-30s / network-changed
// ★ ConnP2P 白名单：no-traffic-30s / p2p-avail-timeout / peer-via-turn / peer-via-ws
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
			reason != "network-changed" {
			return
		}
		log.Printf("[连接] %s 从 LAN 降级 (force: %s)", peerId, reason)
	}

	if rm.states[peerId] == ConnP2P {
		if reason != "no-traffic-30s" &&
			reason != "p2p-avail-timeout" &&
			reason != "peer-via-turn" &&
			reason != "peer-via-ws" {
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

// ShouldRelay LAN / P2P / P2PAvailable 都算「不中继」。
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

// SendToPeer 按当前状态发送数据。
//
// ★ 修复：ConnLAN 分支 LAN 发送失败后降级到 ConnP2PAvailable（不是 ConnP2P）。
//
// 原因：LAN 失败说明对端已不在本 LAN（可能对端换了网络，或本端换了网络）。
// 直接用 UDPAddr 发公网包，WriteTo 可能成功但 NAT 丢包。
// 降级到 ConnP2PAvailable 后：
//   - 不会跳过 nat_hole_instruction（只有 ConnLAN/ConnP2P 才跳过）
//   - 60s 内收不到 PONG/数据帧 → 降级 TURN/WS
//   - 服务端 punchState 能看到 p2pStatus=2，继续协调打洞
func (rm *RelayManager) SendToPeer(peerId string, data []byte, target *PeerInfo) bool {
	state := rm.GetState(peerId)

	switch state {
	case ConnLAN:
		if target == nil || rm.edge == nil || rm.edge.udpConn == nil {
			rm.MarkFallback(peerId)
			return false
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

		// ★ 修复：LAN 失败 → 降级到 ConnP2PAvailable（待验证），不是 ConnP2P
		rm.mu.Lock()
		wasLAN := rm.states[peerId] == ConnLAN
		if wasLAN {
			rm.states[peerId] = ConnP2PAvailable
		}
		rm.mu.Unlock()

		if wasLAN {
			log.Printf("[连接] %s LAN 发送失败，降级到 P2PAvailable（等待验证）", peerId)
			if rm.edge != nil {
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

// ResetP2PStates 重置所有 P2P / LAN 状态，保留 TURN/WS。
//
// 网络切换（WiFi ↔ Cellular / WiFi1 → WiFi2）后：P2P/LAN 的 UDP 路径
// 可能失效，但 TURN 走中继、WS 走 TCP 信令，源 IP 变化不影响，必须保留。
//
// 注意：LanEndpoints 本身由 edge.OnNetworkChanged 清空（对端换了网络，
// 旧 LAN 地址不可达），这里只改状态。
func (rm *RelayManager) ResetP2PStates(reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for peerID, state := range rm.states {
		if state == ConnLAN || state == ConnP2P || state == ConnP2PAvailable {
			rm.states[peerID] = ConnUnknown
			log.Printf("[连接] %s %s → Unknown（%s）", peerID, state, reason)
		}
	}
}
