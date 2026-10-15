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

// setPeerUDPAddr 是 peer.UDPAddr 的唯一写入点。
//
// 所有可能改变 UDPAddr 的入口都必须经过它，避免逻辑分叉：
//   - registerPeer       (joined / ready 消息)
//   - handleP2PFrame     (收到 N2NS 帧)
//   - notePeerCommon     (收到 Punch/ACK/IPv4)
//   - ensureTargetPeer   (nat_hole_instruction)
//
// ★ LAN 优先规则：
//   1. old=nil             → 直接设置
//   2. old=公网 new=LAN    → 更新（LAN 升级）
//   3. old=LAN  new=LAN    → 端口变了才更新
//   4. old=LAN  new=公网   → 保留 LAN，除非 lastLANRecvAt 超过 300 秒
//   5. old=公网 new=公网   → 端口变了才更新
//
// ★ 副作用：
//   - 新地址是 LAN 时刷新 p.lastLANRecvAt
//   - 不更新 p.lastRecvAt（由调用者维护）
//
// ★ 调用约定：
//   调用者必须持有 e.peersMu 写锁。
func (e *Edge) setPeerUDPAddr(p *PeerInfo, newAddr *net.UDPAddr, source string) bool {
	if p == nil || newAddr == nil {
		return false
	}

	now := time.Now().UnixMilli()
	newIsLAN := isPrivateIPv4(newAddr.IP)
	oldIsLAN := p.UDPAddr != nil && isPrivateIPv4(p.UDPAddr.IP)

	shouldUpdate := false
	reason := ""

	if p.UDPAddr == nil {
		shouldUpdate = true
		reason = "init"
	} else if !oldIsLAN && newIsLAN {
		shouldUpdate = true
		reason = "lan-upgrade"
	} else if oldIsLAN && !newIsLAN {
		if p.lastLANRecvAt > 0 && now-p.lastLANRecvAt > 300000 {
			shouldUpdate = true
			reason = "lan-timeout"
		} else {
			reason = "keep-lan"
		}
	} else {
		if p.UDPAddr.Port != newAddr.Port {
			shouldUpdate = true
			reason = "port-change"
		} else {
			reason = "same"
		}
	}

	// 新地址是 LAN 时，无论是否更新 UDPAddr，都要刷新 lastLANRecvAt
	if newIsLAN {
		p.lastLANRecvAt = now
	}

	if shouldUpdate {
		p.UDPAddr = &net.UDPAddr{IP: newAddr.IP, Port: newAddr.Port}
		if reason == "lan-timeout" {
			log.Printf("[P2P] %s LAN 超 300s 无流量，切公网 (source=%s)",
				p.ClientID, source)
		}
		return true
	}
	return false
}

// handleP2PFrame 收到 N2NS 帧时的入口。
//
// ★ 修复：更新 peer.UDPAddr 通过 setPeerUDPAddr 统一处理。
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
			// ★ 统一通过 setPeerUDPAddr 更新（LAN 优先）
			e.setPeerUDPAddr(p, peerAddr, "n2ns")
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
