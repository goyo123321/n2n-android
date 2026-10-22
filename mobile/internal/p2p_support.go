package internal

// P2P stream 依赖的跨平台辅助方法。

import (
	"log"
	"net"
	"time"
)

// findPeerIDByAddr 按对端地址查 peerID。
//
// ★ P0 修复（本次）：加 IP 匹配 AssistedEndpoints
//
// 旧版问题：HardNAT 端口漂移时，B 的 PONG 从 112.96.52.208:任意端口 来，
// 但 AssistedEndpoints 里只有 112.96.52.208:32519/52911/...（固定几个端口）。
// 精确匹配全部失败 → 找不到 peer → lastRecvAt 不更新 → 30 秒后 ForceFallback。
//
// 新版：精确匹配全部失败后，用 IP 匹配 AssistedEndpoints（忽略端口）。
// 只要 IP 匹配，就认定是该 peer 的包。
func (e *Edge) findPeerIDByAddr(addr *net.UDPAddr) string {
	if addr == nil {
		return ""
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()

	// 1. 精确匹配 UDPAddr
	for id, p := range e.peers {
		if p.UDPAddr != nil && p.UDPAddr.IP.Equal(addr.IP) && p.UDPAddr.Port == addr.Port {
			return id
		}
	}

	// 2. 精确匹配 LanEndpoints
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

	// 3. 精确匹配 AssistedEndpoints
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

	// ★ 新增 4：IP 匹配 AssistedEndpoints（HardNAT 端口漂移兜底）
	// B 的 PONG 从 112.96.52.208:任意端口 来，只要 IP 匹配 AssistedEndpoints 就认
	for id, p := range e.peers {
		for _, ass := range p.AssistedEndpoints {
			aa, err := net.ResolveUDPAddr("udp", ass)
			if err != nil {
				continue
			}
			if aa.IP.Equal(addr.IP) {
				return id
			}
		}
	}

	// ★ 新增 5：IP 匹配 UDPAddr（原有兜底，移到后面避免干扰）
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
// 新行为：
//   - 收到 PING → 只更新 lastRecvAt（由调用方已完成），不升级、不回 PONG
//   - 收到 PONG → 升级 P2P（证明双向可达）
//   - 收到数据帧 → 升级 P2P（数据面已验证）
//
// ★ P1 修复：区分 PONG / 数据帧的 viaTURN 来源
//
//   - viaTURN=false 的 PONG → MarkP2P（真 P2P）
//   - viaTURN=true  的 PONG → 保持 P2PAvailable（不标记 P2P），
//                        让 60s 超时自然降级到 TURN
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
