package internal

import (
	"log"
	"net"
	"sync"
	"time"
)

type ConnType string

const (
	ConnP2P     ConnType = "p2p"
	ConnTURN    ConnType = "turn"
	ConnRelay   ConnType = "relay"
	ConnUnknown ConnType = "unknown"
)

type RelayManager struct {
	mu         sync.RWMutex
	ws         *WSTransport
	turnClient *TURNClient
	edge       *Edge
	states     map[string]ConnType
}

func NewRelayManager(ws *WSTransport, turnClient *TURNClient, edge *Edge) *RelayManager {
	return &RelayManager{
		ws:         ws,
		turnClient: turnClient,
		edge:       edge,
		states:     make(map[string]ConnType),
	}
}

func (rm *RelayManager) MarkP2P(peerId string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] != ConnP2P {
		log.Printf("[连接] %s → P2P", peerId)
		rm.states[peerId] = ConnP2P
	}
}

func (rm *RelayManager) MarkFallback(peerId string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] == ConnTURN || rm.states[peerId] == ConnRelay {
		return
	}
	if rm.turnClient != nil && rm.turnClient.IsReady() {
		rm.states[peerId] = ConnTURN
		log.Printf("[连接] %s → TURN", peerId)
		return
	}
	rm.states[peerId] = ConnRelay
	log.Printf("[连接] %s → WS 中继", peerId)
}

func (rm *RelayManager) UpgradeRelaysToTURN() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.turnClient == nil || !rm.turnClient.IsReady() {
		return
	}
	upgraded := 0
	for peerId, state := range rm.states {
		if state == ConnRelay {
			rm.states[peerId] = ConnTURN
			upgraded++
		}
	}
	if upgraded > 0 {
		log.Printf("[连接] 升级 %d 个 WS 中继到 TURN", upgraded)
	}
}

func (rm *RelayManager) DowngradeToWS(peerId string, reason string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.states[peerId] != ConnRelay {
		log.Printf("[连接] %s → WS 中继 (%s)", peerId, reason)
		rm.states[peerId] = ConnRelay
	}
}

func (rm *RelayManager) ShouldRelay(peerId string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	s, ok := rm.states[peerId]
	if !ok {
		return true
	}
	return s == ConnTURN || s == ConnRelay
}

func (rm *RelayManager) GetState(peerId string) ConnType {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if s, ok := rm.states[peerId]; ok {
		return s
	}
	return ConnUnknown
}

func (rm *RelayManager) SendToPeer(peerId string, data []byte, target *PeerInfo) bool {
	state := rm.GetState(peerId)
	switch state {
	case ConnP2P:
		if target != nil && target.UDPAddr != nil {
			_, err := rm.edge.udpConn.WriteToUDP(data, target.UDPAddr)
			if err == nil {
				return true
			}
			log.Printf("[P2P] 发送到 %s 失败: %v", peerId, err)
			rm.MarkFallback(peerId)
		}
		return false
	case ConnTURN:
		if target != nil && target.TurnRelayAddr != "" && rm.turnClient != nil {
			relayAddr, err := net.ResolveUDPAddr("udp", target.TurnRelayAddr)
			if err == nil {
				if err := rm.turnClient.Send(data, relayAddr); err == nil {
					return true
				}
			}
		}
		rm.DowngradeToWS(peerId, "send failed")
		return false
	case ConnRelay:
		err := rm.ws.SendBinary(data)
		return err == nil
	}
	return false
}

func (rm *RelayManager) Report(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		for range ticker.C {
			rm.mu.RLock()
			conns := make(map[string]string)
			for peerId, t := range rm.states {
				conns[peerId] = string(t)
			}
			rm.mu.RUnlock()
			if len(conns) == 0 {
				continue
			}
			_ = rm.ws.Send(map[string]interface{}{
				"type":    "connection_status",
				"payload": map[string]interface{}{"connections": conns},
			})
		}
	}()
}
