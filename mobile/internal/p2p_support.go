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
// ★ 修复：更新 peer.UDPAddr 时采用"LAN 优先 + 60 秒窗口"策略。
//
// 更新规则（peer 已有的 UDPAddr 称为 oldAddr，新收到包的源地址称 newAddr）：
//   1. oldAddr 为空                → 直接设置
//   2. old=公网, new=LAN           → 更新（LAN 优先），刷新 lastLANRecvAt
//   3. old=LAN,  new=LAN           → 端口变了才更新，刷新 lastLANRecvAt
//   4. old=LAN,  new=公网          → 保留 LAN，除非 lastLANRecvAt 超过 60 秒
//   5. old=公网, new=公网          → 端口变了才更新
//
// PING/PONG 语义区分：
//   - 收到 PING  → 只证明"对端→本端"单向可达，不升级本端状态
//   - 收到 PONG  → 双向通道已被证明 → MarkP2P
//   - 其他帧（SYN/SYNACK/DATA/ACK/FIN/RST）→ 直接 MarkP2P
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
				// 无旧地址
				shouldUpdate = true
				if newIsLAN {
					p.lastLANRecvAt = now
				}
			} else if !oldIsLAN && newIsLAN {
				// 旧公网 → 新 LAN（LAN 优先）
				shouldUpdate = true
				p.lastLANRecvAt = now
			} else if oldIsLAN && !newIsLAN {
				// 旧 LAN → 新公网
				// ★ 只有 60 秒内没收到任何 LAN 帧时才切换
				if now-p.lastLANRecvAt > 60000 {
					shouldUpdate = true
					log.Printf("[P2P] %s LAN 地址超过 60s 无流量，改用公网地址", peerID)
				}
			} else if oldIsLAN && newIsLAN {
				// 都是 LAN，端口变了才更新
				if p.UDPAddr.Port != peerAddr.Port {
					shouldUpdate = true
				}
				p.lastLANRecvAt = now   // ★ 每次收到 LAN 帧都刷新
			} else {
				// 都是公网，端口变了才更新
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

	if e.relayMgr != nil {
		switch f.Type {
		case p2pTypePING:
			// 单向证明，不升级本端状态
		case p2pTypePONG:
			if e.relayMgr.GetState(peerID) != ConnP2P {
				log.Printf("[P2P] 收到 PONG → P2P 直连（双向通道已验证）")
				e.relayMgr.MarkP2P(peerID)
			}
		default:
			if e.relayMgr.GetState(peerID) != ConnP2P {
				log.Printf("[P2P] 收到数据帧（stream=%d type=%d）→ P2P 直连",
					f.StreamID, f.Type)
				e.relayMgr.MarkP2P(peerID)
			}
		}
	}

	e.p2pStreams.handleFrame(peerID, peerAddr, viaTURN, f)
}
