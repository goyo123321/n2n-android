package internal

// 网络切换（WiFi ↔ Cellular / WiFi1 → WiFi2）时的状态清理。

import (
	"log"
	"sync/atomic"
	"time"
)

// networkChangeMergeDelay 1.5 秒合并窗口。
//
// Kotlin 侧已经做了 IPv6 link-local 忽略 + 1.5s 去抖，这里是双保险：
// 即使 Kotlin 侧漏掉（比如 onAvailable 触发），也不会连续执行七步清理。
const networkChangeMergeDelay = 1500 * time.Millisecond

// OnNetworkChanged 网络切换入口。
//
// 改造要点：
//   - 旧版每次调用立即执行七步清理，Kotlin 侧 IPv4/IPv6 交替触发时
//     4 秒 6 次，状态机和数据面反复被打断。
//   - 新版 1.5 秒合并，只执行最后一次。
//     - 4 秒 6 次 → 4 秒 1 次
//     - 真实切换不丢（最后一次一定是最终网络状态）
//     - 代价：真实切换晚 1.5 秒执行（切换期间走 WS 兜底，可接受）
func (e *Edge) OnNetworkChanged(networkType string) {
	e.pendingNetChangeMu.Lock()
	e.pendingNetChange = networkType
	if e.pendingNetChangeT != nil {
		e.pendingNetChangeT.Stop()
	}
	e.pendingNetChangeT = time.AfterFunc(networkChangeMergeDelay, func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Network] pending apply panic: %v", r)
			}
		}()
		e.pendingNetChangeMu.Lock()
		t := e.pendingNetChange
		e.pendingNetChange = ""
		e.pendingNetChangeT = nil
		e.pendingNetChangeMu.Unlock()
		if t != "" {
			e.applyNetworkChanged(t)
		}
	})
	e.pendingNetChangeMu.Unlock()

	log.Printf("[Network] %s 已排队（1.5s 内合并）", networkType)
}

// applyNetworkChanged 实际执行七步清理 + 立即上报 + STUN 重探。
func (e *Edge) applyNetworkChanged(networkType string) {
	log.Printf("[Network] 执行网络变化处理: %s", networkType)

	e.mu.Lock()
	e.lastNetworkChangedAt = time.Now().UnixMilli()
	e.lastNetworkType = networkType
	e.mu.Unlock()

	// 1. 清空旧 natMeta
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
		if oldEndpoint != "" {
			log.Printf("[Network] 清空旧 natMeta.PublicEndpoint=%q", oldEndpoint)
		}
	}
	e.mu.Unlock()

	// 2. 清打洞失败计数
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

	// 3. 重置 relayMgr（LAN/P2P/P2PAvailable/TURN → Unknown，清 failCounts）
	if e.relayMgr != nil {
		e.relayMgr.ResetP2PStates("network-changed")
	}

	// 4. 清所有 peer
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

	// 6. 立即上报 metadata（空地址）
	atomic.StoreInt64(&e.lastMetadataReportAt, 0)
	e.reportMetadata()

	// 7. 通知服务端
	if e.ws != nil {
		_ = e.ws.Send(map[string]interface{}{
			"type":   "network_changed",
			"reason": networkType,
		})
		log.Printf("[Network] 已通知服务端重新协调")
	}

	// 8. 立即上报 connection_status
	//
	// 旧版靠 RelayManager.Report(10s) 周期，切换后 10 秒内服务端看到的
	// 还是旧的 connections（比如全 p2p），runCoordination 认为"P2P 正常"
	// 跳过打洞。立即上报后服务端立刻看到 Unknown，触发 punchState 退役。
	if e.relayMgr != nil {
		e.relayMgr.ForceReportNow()
	}

	// 9. 触发 STUN 重探
	//
	// 旧版 natType 只在 Start() 时探一次，切换后永远 "unknown"，
	// 面板显示未知，classifyNat 把 EasyNAT 也当 HardNAT 扫描（更慢）。
	safeGo("nat-reprobe", func() {
		time.Sleep(500 * time.Millisecond)
		if e.udpConn == nil {
			return
		}
		meta := probeNAT(e.udpPort, nil)
		if meta == nil {
			return
		}
		e.mu.Lock()
		e.natMeta = meta
		e.mu.Unlock()
		log.Printf("[NAT] 重探测完成: %s pub=%s", meta.NATType, meta.PublicEndpoint)
		e.reportMetadata()
	})
}
