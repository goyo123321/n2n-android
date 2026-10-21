package internal

// P2P stream 依赖的跨平台辅助方法。

import (
	"log"
	"net"
	"time"
)

func (e *Edge) findPeerIDByAddr(addr *net.UDPAddr) string {
	if addr == nil {
		return ""
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()

	for id, p := range e.peers {
		if p.UDPAddr != nil && p.UDPAddr.IP.Equal(addr.IP) && p.UDPAddr.Port == addr.Port {
			return id
		}
	}
	for id, p := range e.peers {
		for _, lan := range p.LanEndpoints {
			la, err := net.ResolveUDPAddr("udp", lan)
			if err != nil {
				continue
			}
			if la.IP.Equal(addr.IP) && la.Port == addr.Port {
				return id
			}
		}
	}
	for id, p := range e.peers {
		for _, ass := range p.AssistedEndpoints {
			aa, err := net.ResolveUDPAddr("udp", ass)
			if err != nil {
				continue
			}
			if aa.IP.Equal(addr.IP) && aa.Port == addr.Port {
				return id
			}
		}
	}
	for id, p := range e.peers {
		if p.UDPAddr != nil && p.UDPAddr.IP.Equal(addr.IP) {
			return id
		}
	}
	return ""
}

func (e *Edge) peerAddrAndMode(peerID string) (*net.UDPAddr, bool) {
	e.peersMu.RLock()
	p := e.peers[peerID]
	e.peersMu.RUnlock()
	if p == nil || p.UDPAddr == nil {
		return nil, false
	}
	viaTURN := true
	if e.relayMgr != nil {
		st := e.relayMgr.GetState(peerID)
		viaTURN = st != ConnP2P && st != ConnP2PAvailable
	}
	return p.UDPAddr, viaTURN
}

// handleP2PFrame 收到 N2NS 帧时的入口。
//
// ★ P0 修复：PING/PONG 状态机（与 PC / Termux 对齐）
//
// 旧 Android 行为：
//   - 收到 PING → 回 PONG + 自己升 P2P
//   - 后果：单向 P2P（A→B 通、B→A 断）时 B 误以为双向可达，
//           向 A 发数据全部丢失，卡在 P2P 丢包，无法降级
//
// 新行为：
//   - 收到 PING → 只更新 lastRecvAt（由调用方已完成），不升级、不回 PONG
//   - 收到 PONG → 升级 P2P（证明双向可达）
//   - 收到数据帧 → 升级 P2P（数据面已验证）
//
// ★ P1 修复（本次）：区分 PONG / 数据帧的 viaTURN 来源
//
//   旧版：无论 PONG 从 UDP 直连还是 TURN 到达，都 MarkP2P。
//         跨网络场景下，PONG 只能通过 TURN 到达，但仍被标记成 P2P，
//         导致面板显示假 p2p，实际数据走 TURN。
//
//   新版：
//     - viaTURN=false 的 PONG → MarkP2P（真 P2P）
//     - viaTURN=true  的 PONG → 保持 P2PAvailable（不标记 P2P），
//                          让 60s 超时自然降级到 TURN
func (e *Edge) handleP2PFrame(peerAddr *net.UDPAddr, viaTURN bool, data []byte) {
	if e.p2pStreams == nil {
		return
	}
	f, err := decodeP2PFrame(data)
	if err != nil {
		return
	}
	peerID := e.findPeerIDByAddr(peerAddr)
	if peerID == "" {
		return
	}

	// 更新 UDPAddr（LAN 优先）
	if !viaTURN {
		e.peersMu.Lock()
		if p := e.peers[peerID]; p != nil {
			now := time.Now().UnixMilli()
			newIsLAN := isPrivateIPv4(peerAddr.IP)
			oldIsLAN := p.UDPAddr != nil && isPrivateIPv4(p.UDPAddr.IP)

			shouldUpdate := false
			if p.UDPAddr == nil {
				shouldUpdate = true
				if newIsLAN {
					p.lastLANRecvAt = now
				}
			} else if !oldIsLAN && newIsLAN {
				shouldUpdate = true
				p.lastLANRecvAt = now
			} else if oldIsLAN && !newIsLAN {
				if p.lastLANRecvAt > 0 && now-p.lastLANRecvAt > 30000 {
					shouldUpdate = true
					log.Printf("[P2P] %s LAN 地址超过 30s 无流量，改用公网地址", peerID)
				}
			} else if oldIsLAN && newIsLAN {
				if p.UDPAddr.Port != peerAddr.Port {
					shouldUpdate = true
				}
				p.lastLANRecvAt = now
			} else {
				if p.UDPAddr.Port != peerAddr.Port {
					shouldUpdate = true
				}
			}

			if shouldUpdate {
				p.UDPAddr = &net.UDPAddr{IP: peerAddr.IP, Port: peerAddr.Port}
			}
			p.lastRecvAt = now
		}
		e.peersMu.Unlock()
	}

	// ★ P0 + P1 修复：区分 viaTURN 来源
	if e.relayMgr != nil {
		switch f.Type {
		case p2pTypePING:
			// 静默：只更新 lastRecvAt（上方已完成）
			// PONG 由 p2pStreams.handleFrame 统一回复

		case p2pTypePONG:
			if e.relayMgr.GetState(peerID) != ConnP2P {
				if !viaTURN {
					log.Printf("[P2P] 收到 PONG（UDP 直连）→ P2P 直连（双向通道已验证）")
					e.relayMgr.MarkP2P(peerID)
				} else {
					// ★ P1 修复：通过 TURN 收到的 PONG → 不标记 P2P
					//   原因：PONG 走 TURN 说明对端到本端可能单向不通，
					//         本端写 LAN 地址是黑洞，只有 TURN 能到达。
					//   行为：保持 P2PAvailable，60s 超时后自然降级到 TURN。
					log.Printf("[TURN] 通过 TURN 收到 PONG → 保持 P2PAvailable（不标记 P2P，等待直连 PONG）")
				}
			}

		default:
			if e.relayMgr.GetState(peerID) != ConnP2P {
				if !viaTURN {
					log.Printf("[P2P] 收到数据帧（stream=%d type=%d）→ P2P 直连",
						f.StreamID, f.Type)
					e.relayMgr.MarkP2P(peerID)
				} else {
					log.Printf("[TURN] 通过 TURN 收到数据帧（stream=%d type=%d）→ 保持当前状态",
						f.StreamID, f.Type)
				}
			}
		}
	}

	e.p2pStreams.handleFrame(peerID, peerAddr, viaTURN, f)
}
