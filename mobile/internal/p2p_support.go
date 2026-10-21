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
// ★ 三端统一（本次修复）：PING/PONG 状态机对齐 PC/Termux。
//
// 旧 Android 行为：
//   - 收到 PING → 回 PONG + 自己升 P2P
//   - 后果：单向 P2P（A→B 通、B→A 断）时 B 误以为双向可达，
//           向 A 发数据全部丢失，卡在 P2P 丢包，无法降级
//
// 新行为（对齐 PC/Termux）：
//   - 收到 PING → 只更新 lastRecvAt（由调用方已完成），不升级、不回 PONG
//   - 收到 PONG → 升级 P2P（证明双向可达）
//   - 收到数据帧 → 升级 P2P（数据面已验证）
//   - PONG 由 p2pStreams.handleFrame 统一回复（避免双 PONG）
//
// 单向 P2P 场景下，B 保持 ConnP2PAvailable，
// 60s 后由 scheduleP2PAvailTimeout 触发 ForceFallback → 走 TURN/WS，
// 比"卡在 P2P 丢包"可靠得多。
//
// ★ LAN 优先 + 30 秒窗口 + lastLANRecvAt > 0 检查
//
// ★★ 补丁 2：LAN→公网切换窗口 300000（5 分钟）→ 30000（30 秒）。
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
				// ★★ 补丁 2：5 分钟 → 30 秒
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

	// ★ 状态处理：PING/PONG 逻辑对齐 PC/Termux
	if e.relayMgr != nil {
		switch f.Type {
		case p2pTypePING:
			// ★ 三端统一：收到 PING 只更新 lastRecvAt（上方已完成），
			// 不升级 P2P、不回 PONG。PONG 由 p2pStreams.handleFrame 负责。
			//
			// 原因：PING 只证明"对方能发到我"，不证明"我能发到对方"。
			// 只有收到 PONG（对方能收到我的 PING 并回复）才证明双向可达。
			// 修复前 Android 收到 PING 就升级，导致单向 P2P 场景丢包。

		case p2pTypePONG:
			// 兼容旧版对端：收到 PONG 升 P2P
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
