package internal

// P2P stream 依赖的跨平台辅助方法。

import (
	"log"
	"net"
	"time"
)

// findPeerIDByAddr 按对端地址查 peerID。
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

	// Assisted IP 匹配（唯一命中才返回）
	var assistMatch string
	var assistCount int
	for id, p := range e.peers {
		for _, ass := range p.AssistedEndpoints {
			aa, err := net.ResolveUDPAddr("udp", ass)
			if err != nil {
				continue
			}
			if aa.IP.Equal(addr.IP) {
				if assistMatch != id {
					assistMatch = id
					assistCount++
				}
				break
			}
		}
	}
	if assistCount == 1 {
		return assistMatch
	}

	// UDPAddr IP 匹配（唯一命中才返回）
	var udpMatch string
	var udpCount int
	for id, p := range e.peers {
		if p.UDPAddr != nil && p.UDPAddr.IP.Equal(addr.IP) {
			if udpMatch != id {
				udpMatch = id
				udpCount++
			}
		}
	}
	if udpCount == 1 {
		return udpMatch
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
		viaTURN = st != ConnLAN && st != ConnP2P && st != ConnP2PAvailable
	}
	return p.UDPAddr, viaTURN
}

// handleP2PFrame 收到 N2NS 帧时的入口。
//
// ★ 修复：更新 lastPublicRecvAt（公网包）
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
				lastLAN := p.lastLANRecvAt
				if lastLAN == 0 {
					lastLAN = p.registeredAt
				}
				if lastLAN > 0 && now-lastLAN > 30000 {
					shouldUpdate = true
					log.Printf("[P2P] %s LAN 地址超过 30s 无流量，改用公网地址", peerID)
				}
			} else if oldIsLAN && newIsLAN {
				if p.UDPAddr.IP.Equal(peerAddr.IP) && p.UDPAddr.Port != peerAddr.Port {
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
			// ★ 修复：公网包更新 lastPublicRecvAt
			if !newIsLAN {
				p.lastPublicRecvAt = now
			}
		}
		e.peersMu.Unlock()
	}

	if e.relayMgr != nil {
		cur := e.relayMgr.GetState(peerID)
		fromLAN := !viaTURN && isPrivateIPv4(peerAddr.IP)

		switch f.Type {
		case p2pTypePING:
			// 静默：只更新 lastRecvAt

		case p2pTypePONG:
			if viaTURN && (cur == ConnLAN || cur == ConnP2P) {
				log.Printf("[P2P] %s 通过 TURN 收到 PONG → 对端已降级，本机跟进", peerID)
				e.relayMgr.ForceFallback(peerID, "peer-via-turn")
			} else if fromLAN {
				if cur != ConnLAN {
					log.Printf("[LAN] 收到 PONG（LAN）→ LAN 直连")
					e.relayMgr.MarkLAN(peerID)
				}
			} else if !viaTURN && cur != ConnLAN && cur != ConnP2P {
				log.Printf("[P2P] 收到 PONG（公网）→ P2P 直连（双向通道已验证）")
				e.relayMgr.MarkP2P(peerID)
			} else if viaTURN && cur != ConnLAN && cur != ConnP2P {
				log.Printf("[TURN] 通过 TURN 收到 PONG → 保持 P2PAvailable")
			}

		default:
			if viaTURN && (cur == ConnLAN || cur == ConnP2P) {
				log.Printf("[P2P] %s 通过 TURN 收到数据帧 → 对端已降级，本机跟进", peerID)
				e.relayMgr.ForceFallback(peerID, "peer-via-turn")
			} else if fromLAN {
				if cur != ConnLAN {
					log.Printf("[LAN] 收到数据帧（LAN）→ LAN 直连")
					e.relayMgr.MarkLAN(peerID)
				}
			} else if !viaTURN && cur != ConnLAN && cur != ConnP2P {
				log.Printf("[P2P] 收到数据帧（公网）→ P2P 直连")
				e.relayMgr.MarkP2P(peerID)
			} else if viaTURN && cur != ConnLAN && cur != ConnP2P {
				log.Printf("[TURN] 通过 TURN 收到数据帧 → 保持当前状态")
			}
		}
	}

	e.p2pStreams.handleFrame(peerID, peerAddr, viaTURN, f)
}
