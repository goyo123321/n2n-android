package internal

// 网络切换（WiFi ↔ Cellular）时的状态清理。
//
// 为什么需要：
//   WiFi 切移动网络后，UDP socket 源 IP 变化，旧 P2P 路径必然失效。
//   但 STUN 探测周期 5 秒 + reportMetadata 节流 30 秒 → 中间 5-35 秒
//   客户端继续往旧路径发包全丢。
//
// 用 ConnectivityManager.NetworkCallback 毫秒级感知，立即清状态 + 通知
// 服务端重新协调。
//
// ★ LAN 保留：LanEndpoints 不清空，2s 保活会重新探测。LAN 路径不依赖
//   UDP 源 IP（走 L2 桥接），网络切换后可能仍然可达。

import (
	"log"
	"sync/atomic"
	"time"
)

// OnNetworkChanged 网络切换时的状态清理。
//
// 分六步：
//  1. 清空打洞失败计数
//  2. 重置 relayMgr 的 P2P/LAN 状态（保留 TURN/WS）
//  3. 清空 peer 的 UDPAddr（LanEndpoints 保留）
//  4. 取消所有 pending timer
//  5. 立即上报 metadata（绕过 30s 节流）
//  6. 通知服务端重新协调
func (e *Edge) OnNetworkChanged(networkType string) {
	log.Printf("[Network] 检测到网络变化: %s，重置 P2P 状态", networkType)

	// 1. 清空打洞失败计数
	natHoleActiveMu.Lock()
	failCount := len(failCounts)
	if failCount > 0 {
		failCounts = make(map[string]int)
	}
	nActive := len(natHoleActive)
	if nActive > 0 {
		natHoleActive = make(map[string]bool)
	}
	natHoleActiveMu.Unlock()

	// 2. 重置 relayMgr 的 P2P/LAN 状态（保留 TURN/WS）
	if e.relayMgr != nil {
		e.relayMgr.ResetP2PStates("network-changed")
	}

	// 3. 清空所有 peer 的 UDPAddr（LanEndpoints 保留，2s 保活会重新探测）
	e.peersMu.Lock()
	count := 0
	for _, p := range e.peers {
		if p.UDPAddr != nil {
			count++
		}
		p.UDPAddr = nil
		p.lastLANRecvAt = 0
		p.lastRecvAt = 0
		// ★ LanEndpoints 不清空 —— LAN 路径不依赖 UDP 源 IP，
		//   网络切换后（如 WiFi 切移动网络）同 WiFi peer 可能仍可达
	}
	e.peersMu.Unlock()
	log.Printf("[Network] 清空 %d 个 peer 的 UDPAddr（保留 LanEndpoints 重新探测）", count)

	// 4. 取消所有 pending timer
	e.fallbackTimersMu.Lock()
	for _, t := range e.fallbackTimers {
		t.Stop()
	}
	e.fallbackTimers = make(map[string]*time.Timer)
	e.fallbackTimersMu.Unlock()

	e.p2pAvailTimersMu.Lock()
	for _, t := range e.p2pAvailTimers {
		t.Stop()
	}
	e.p2pAvailTimers = make(map[string]*time.Timer)
	e.p2pAvailTimersMu.Unlock()

	// 5. 重置 metadata 上报节流，立即上报
	atomic.StoreInt64(&e.lastMetadataReportAt, 0)
	e.reportMetadata()

	// 6. 通知服务端重新协调（让服务端 clearPairStateFor + 广播）
	if e.ws != nil {
		_ = e.ws.Send(map[string]interface{}{
			"type":   "network_changed",
			"reason": networkType,
		})
		log.Printf("[Network] 已通知服务端重新协调")
	}
}
