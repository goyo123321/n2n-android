package internal

import (
	"log"
	"sync/atomic"
	"time"
)

const networkChangeMergeDelay = 1500 * time.Millisecond

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

	// 3. 重置 relayMgr
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
	if e.relayMgr != nil {
		e.relayMgr.ForceReportNow()
	}

	// 9. 触发 STUN 重探
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

	// ★ 10. 立即重建 TURN allocation
	//
	// 场景：客户端换网络（WiFi → Cellular），旧 allocation 基于旧 NAT 映射，
	// 在 TURN 服务器侧失效。但 UDP socket 本身没关，SendTo 返回 nil（假成功），
	// 数据丢失。旧版靠 Refresh 失败 3 次（最长 3 分钟）才触发重建。
	//
	// 修复：主动重建 allocation。重建期间 IsReady()=false，
	// SendToPeer 的 ConnTURN 分支检测到后走 WS 兜底。
	safeGo("turn-rebuild", func() {
		time.Sleep(500 * time.Millisecond)
		if e.turnClient == nil {
			return
		}
		ctx, cancel := contextWithTimeout(30 * time.Second)
		defer cancel()

		log.Printf("[TURN] 网络变化，开始重建 allocation...")
		if err := e.turnClient.RebuildAllocation(ctx); err != nil {
			log.Printf("[TURN] 网络变化后重建失败: %v（走 WS 兜底，30 秒后 StartReconnectLoop 重试）", err)
			return
		}
		log.Printf("[TURN] ✅ 网络变化后重建成功: %s", e.turnClient.GetRelayAddr())

		if e.ws != nil {
			_ = e.ws.Send(map[string]interface{}{
				"type":      "turn_relay_info",
				"relayAddr": e.turnClient.GetRelayAddr(),
			})
			log.Printf("[TURN] 已上报新 relay 地址给服务端")
		}

		if e.relayMgr != nil {
			e.relayMgr.UpgradeRelaysToTURN()
		}
	})
}
