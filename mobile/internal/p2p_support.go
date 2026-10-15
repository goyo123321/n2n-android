package internal

// P2P stream 依赖的跨平台辅助方法 + UDPAddr 统一更新。

import (
	"log"
	"net"
	"time"
)

// ============ 常量 ============

// lanPreferredWindowMs 是"LAN 优先"窗口。
//
// 300 秒内收到过 LAN 帧，就认为 LAN 通道可用，优先走 LAN。
// 超过 300 秒，允许切回公网。
//
// 300 秒 = 20 个 PING 周期（15 秒一个）。容忍偶发丢包，
// 但不会因一次抖动就永久卡公网。
const lanPreferredWindowMs = 300000

// ============ UDPAddr 统一更新 ============

// updatePeerUDPAddrLocked 是 UDPAddr 更新的**唯一入口**（已持有 peersMu 锁）。
//
// 所有 UDPAddr 更新都必须经过此函数，确保"LAN 优先"策略一致。
//
// 参数：
//   - p:       目标 peer（不为 nil）
//   - newAddr: 新地址（LAN 或公网）
//   - source:  调用来源，用于日志（"join" / "n2ns" / "probe" / "instr"）
//
// 返回：
//   - bool: 是否实际更新了 UDPAddr
//
// 行为：
//   - newAddr 是 LAN → 总是刷新 lastLANRecvAt
//   - p 当前无 UDPAddr → 直接设置
//   - p 当前是公网，newAddr 是 LAN → 更新（LAN 优先）
//   - p 当前是 LAN，newAddr 是公网 → 保留 LAN（除非 lastLANRecvAt 超窗口）
//   - p 当前是 LAN，newAddr 是 LAN → 端口变了才更新
//   - p 当前是公网，newAddr 是公网 → 端口变了才更新
//
// 不更新：
//   - lastRecvAt（由调用方负责，每个 packet 都刷新）
//   - hasRealData（同上）
//   - VirtualIP / PubIP / PubPort / NATType / Endpoints（由具体入口处理）
func updatePeerUDPAddrLocked(p *PeerInfo, newAddr *net.UDPAddr, source string) bool {
	if p == nil || newAddr == nil || newAddr.IP == nil {
		return false
	}

	now := time.Now().UnixMilli()
	newIsLAN := isPrivateIPv4(newAddr.IP)
	oldIsLAN := p.UDPAddr != nil && isPrivateIPv4(p.UDPAddr.IP)

	// 收到 LAN 帧：总是刷新 lastLANRecvAt
	if newIsLAN {
		p.lastLANRecvAt = now
	}

	shouldUpdate := false

	if p.UDPAddr == nil {
		// 无旧地址 → 直接设置
		shouldUpdate = true
	} else if !oldIsLAN && newIsLAN {
		// 旧公网 → 新 LAN：LAN 优先
		shouldUpdate = true
	} else if oldIsLAN && !newIsLAN {
		// 旧 LAN → 新公网：只在 lastLANRecvAt 超窗口时才切
		if p.lastLANRecvAt > 0 && now-p.lastLANRecvAt > lanPreferredWindowMs {
			shouldUpdate = true
			log.Printf("[P2P] %s (%s) LAN 超 %ds 无流量，切公网 %s",
				p.ClientID, source, lanPreferredWindowMs/1000, newAddr.String())
		}
	} else {
		// 同类型（LAN→LAN 或 公网→公网）：端口变了才更新
		if p.UDPAddr.Port != newAddr.Port {
			shouldUpdate = true
		}
	}

	if shouldUpdate {
		p.UDPAddr = &net.UDPAddr{IP: newAddr.IP, Port: newAddr.Port}
	}

	return shouldUpdate
}

// isLANPreferredLocked 判断 peer 是否应优先走 LAN（已持有 peersMu 锁）。
//
// 判据：lastLANRecvAt > 0 且距今 < lanPreferredWindowMs。
func isLANPreferredLocked(p *PeerInfo) bool {
	if p == nil || p.lastLANRecvAt <= 0 {
		return false
	}
	return time.Now().UnixMilli()-p.lastLANRecvAt < lanPreferredWindowMs
}

// ============ P2P stream 依赖的方法 ============

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
// ★ 统一调度：UDPAddr 更新走 updatePeerUDPAddrLocked
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
			// ★ 统一走 updatePeerUDPAddrLocked
			updatePeerUDPAddrLocked(p, peerAddr, "n2ns")
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
