package internal

// 网络切换（WiFi ↔ Cellular / WiFi1 → WiFi2）时的状态清理。

import (
	"log"
	"sync/atomic"
	"time"
)

// OnNetworkChanged 网络切换时的状态清理。
//
// 触发点：
//   - Kotlin NetworkCallback.onAvailable（网络类型变化）
//   - Kotlin NetworkCallback.onLinkPropertiesChanged（网关变化 WiFi1 → WiFi2）
//
// 分六步：
//  1. 清空打洞失败计数
//  2. 重置 relayMgr 的 P2P/LAN 状态（保留 TURN/WS）
//  3. 清空 peer 的 UDPAddr / LanEndpoints / lastPublicRecvAt
//  4. 取消所有 pending timer
//  5. 立即上报 metadata（绕过 30s 节流）
//  6. 通知服务端重新协调
func (e *Edge) OnNetworkChanged(networkType string) {
	log.Printf("[Network] 检测到网络变化: %s，重置 P2P 状态", networkType)

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

	if e.relayMgr != nil {
		e.relayMgr.ResetP2PStates("network-changed")
	}

	// ★ 清空 UDPAddr / LanEndpoints / lastPublicRecvAt
	e.peersMu.Lock()
	count := 0
	for _, p := range e.peers {
		if p.UDPAddr != nil {
			count++
		}
		p.UDPAddr = nil
		p.lastLANRecvAt = 0
		p.lastRecvAt = 0
		p.lastPublicRecvAt = 0
		p.LanEndpoints = nil
	}
	e.peersMu.Unlock()
	log.Printf("[Network] 清空 %d 个 peer 的 UDPAddr / LanEndpoints", count)

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

	atomic.StoreInt64(&e.lastMetadataReportAt, 0)
	e.reportMetadata()

	if e.ws != nil {
		_ = e.ws.Send(map[string]interface{}{
			"type":   "network_changed",
			"reason": networkType,
		})
		log.Printf("[Network] 已通知服务端重新协调")
	}
}
