package internal

// P2P stream 依赖的跨平台辅助方法。

import (
	"log"
	"net"
	"time"
)

// findPeerIDByAddr 按对端地址查 peerID。
//
// 四层匹配：
//   1. UDPAddr 精确
//   2. LAN 精确
//   3. Assisted 精确
//   4. UDPAddr IP 相同（兜底）
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
// PING/PONG 语义区分：
//   - 收到 PING  → 只回 PONG，不升级本端状态（单向证明）
//   - 收到 PONG  → 双向通道已被证明 → MarkP2P
//   - 其他帧（SYN/SYNACK/DATA/ACK/FIN/RST）→ 直接 MarkP2P
//
// 收到非 TURN 中继的 N2NS 帧都刷新 p.UDPAddr，避免 Keepalive 打到旧端口。
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
			p.UDPAddr = &net.UDPAddr{IP: peerAddr.IP, Port: peerAddr.Port}
			p.lastRecvAt = time.Now().UnixMilli()
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
