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
// 分七步：
//  1. ★ 清空旧 natMeta（网络变了，旧地址/端口必然失效）
//  2. 清空打洞失败计数
//  3. 重置 relayMgr 的 P2P/LAN/TURN 状态（保留 WS）
//  4. 清空 peer 的 UDPAddr / LanEndpoints / lastP2PRecvAt / TurnRelayAddr
//  5. 取消所有 pending timer
//  6. 立即上报 metadata（此时 PublicEndpoint 为空，不会报旧地址）
//  7. 通知服务端重新协调
func (e *Edge) OnNetworkChanged(networkType string) {
	log.Printf("[Network] 检测到网络变化: %s，重置 P2P 状态", networkType)

	// ★ 修复 1：清空旧 natMeta（网络变了，旧地址/端口必然失效）
	//
	// 场景：A 从运营商 X WiFi 换到运营商 Y WiFi。
	//   旧版：natMeta.PublicEndpoint 还是运营商 X 的地址 → reportMetadata
	//        上报旧地址 → 服务端存旧地址 → B 打洞到旧地址失败
	//        → 5~30 秒黑洞
	//   新版：清空 PublicEndpoint → STUN 首次响应时（oldIP == ""）
	//        通过 maybeResetOwnFailCounts 立即更新 + addAssistedEndpoint
	//        的 wasEmpty 分支立即上报
	e.mu.Lock()
	if e.natMeta != nil {
		oldEndpoint := e.natMeta.PublicEndpoint
		e.natMeta.PublicEndpoint = ""
		e.natMeta.P2PEndpoint = ""
		e.natMeta.AllEndpoints = nil
		e.natMeta.NATType = "unknown"
		e.natMeta.Behavior = "BehaviorPortChanged"
		e.natMeta.MultiExit = false
		e.natMeta.PortsDifference = 0
		e.natMeta.RegularPortsChange = false
		log.Printf("[Network] 清空旧 natMeta.PublicEndpoint=%q", oldEndpoint)
	}
	e.mu.Unlock()

	// 2. 清空打洞失败计数
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

	// 3. 重置 relayMgr 的 P2P/LAN/TURN 状态（保留 WS）
	if e.relayMgr != nil {
		e.relayMgr.ResetP2PStates("network-changed")
	}

	// 4. 清空所有 peer 的 UDPAddr / LanEndpoints / lastP2PRecvAt / TurnRelayAddr
	//
	// ★ 关键：LanEndpoints 也要清空
	//   场景：WiFi1 → WiFi2（不同网段），对端的 LAN 地址（192.168.1.20）
	//   在新网络下不可达。不清空的话，SendToPeer 会先尝试旧 LAN 地址：
	//   - WriteTo 可能返回 nil（内核接受），但包实际丢了
	//   - 不会立即失败降级，延迟更高
	//   清空后，等下次 joined 或 nat_hole_instruction 重新补充。
	//
	// ★ TurnRelayAddr 也清空：
	//   本端换网络后自己的 TURN allocation 失效，对端 relay 地址也失效，
	//   但保留对端 relay 地址（用于跨网络后重连）没意义，因为新旧网络
	//   的 UDP 源 IP 变化后，旧 TURN allocation 全失效。
	e.peersMu.Lock()
	count := 0
	for _, p := range e.peers {
		if p.UDPAddr != nil {
			count++
		}
		p.UDPAddr = nil
		p.lastLANRecvAt = 0
		p.lastPublicRecvAt = 0
		p.lastP2PRecvAt = 0
		p.lastRecvAt = 0
		p.LanEndpoints = nil
		p.TurnRelayAddr = ""
	}
	e.peersMu.Unlock()
	log.Printf("[Network] 清空 %d 个 peer 的 UDPAddr/LanEndpoints/TurnRelayAddr", count)

	// 5. 取消所有 pending timer
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

	// 6. 重置 metadata 上报节流，立即上报
	//   此时 PublicEndpoint 已被清空，上报的是「空地址」
	//   服务端收到后 peer.pubSocket = ""，等待下次 STUN 上报新地址
	atomic.StoreInt64(&e.lastMetadataReportAt, 0)
	e.reportMetadata()

	// 7. 通知服务端重新协调
	if e.ws != nil {
		_ = e.ws.Send(map[string]interface{}{
			"type":   "network_changed",
			"reason": networkType,
		})
		log.Printf("[Network] 已通知服务端重新协调")
	}
}
