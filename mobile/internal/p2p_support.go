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
// ★ 核心逻辑：
//
//   - 收到 PING：
//     - 本端非 P2P → 回一个 PONG（让对端升级）+ 本端也升 P2P
//     - 本端已是 P2P → 不回（双向 PING 已保活，节省流量）
//
//   - 收到 PONG：升 P2P（兼容旧版对端）
//
//   - 收到数据帧（SYN/SYNACK/DATA/ACK/FIN/RST）：升 P2P
//
// ★ LAN 优先 + 300 秒窗口 + lastLANRecvAt > 0 检查
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
				if p.lastLANRecvAt > 0 && now-p.lastLANRecvAt > 300000 {
					shouldUpdate = true
					log.Printf("[P2P] %s LAN 地址超过 300s 无流量，改用公网地址", peerID)
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

	// 状态处理
	if e.relayMgr != nil {
		switch f.Type {
		case p2pTypePING:
			curState := e.relayMgr.GetState(peerID)

			if curState != ConnP2P {
				// ★ 本端非 P2P → 回一个 PONG 让对端升级
				log.Printf("[P2P] 收到 PING（当前 %s）→ 回 PONG + 升 P2P", curState)

				// 先回 PONG（让对端也能升）
				safeGo("p2p-pong-first", func() {
					resp := &p2pFrame{Type: p2pTypePONG, StreamID: 0}
					tmp := &P2PStream{
						ID:       0,
						PeerID:   peerID,
						peerAddr: peerAddr,
						viaTURN:  viaTURN,
					}
					_ = e.sendP2PFrame(tmp, resp)
				})

				// 本端也升 P2P（PING 到达本身就是证据）
				e.relayMgr.MarkP2P(peerID)
			} else {
				// ★ 本端已是 P2P → 不回 PONG（双向 PING 已保活）
				// 静默丢弃
			}

		case p2pTypePONG:
			// 兼容旧版对端：收到 PONG 也升 P2P
			if e.relayMgr.GetState(peerID) != ConnP2P {
				log.Printf("[P2P] 收到 PONG → P2P 直连（双向通道已验证）")
				e.relayMgr.MarkP2P(peerID)
			}

		default:
			// 数据帧 → 升 P2P
			if e.relayMgr.GetState(peerID) != ConnP2P {
				log.Printf("[P2P] 收到数据帧（stream=%d type=%d）→ P2P 直连",
					f.StreamID, f.Type)
				e.relayMgr.MarkP2P(peerID)
			}
		}
	}

	e.p2pStreams.handleFrame(peerID, peerAddr, viaTURN, f)
}
