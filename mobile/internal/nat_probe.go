package internal

import (
	"log"
	"net"
	"strconv"
	"time"

	"github.com/pion/stun/v2"
)

type NATMetadata struct {
	P2PEndpoint        string
	PublicEndpoint     string
	AllEndpoints       []string // ★ 所有 STUN 结果（去重）
	NATType            string
	PortsDifference    int
	RegularPortsChange bool
	Behavior           string
	MultiExit          bool
}

var stunServersHardcoded = []string{
	"74.125.250.129:19302",
	"74.125.204.127:19302",
	"162.159.207.0:3478",
}

var stunServersDomain = []string{
	"stun.l.google.com:19302",
	"stun.cloudflare.com:3478",
	"stun.miwifi.com:3478",
	"stun.chat.bilibili.com:3478",
	"stun.hitv.com:3478",
}

const minStunSamples = 2
const stunProbeTotalTimeout = 5 * time.Second // ★ 总探测时间上限

type natProbeResult struct {
	ip   string
	port int
}

func probeNATWithConn(conn net.PacketConn, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint: "",
		NATType:    "unknown",
		Behavior:   "BehaviorPortChanged",
	}

	if conn == nil {
		log.Printf("[NAT] 无 protected socket，回退默认方法")
		return probeNAT(0, stunServers)
	}

	servers := stunServers
	if len(servers) == 0 {
		servers = append(servers, stunServersHardcoded...)
		servers = append(servers, stunServersDomain...)
	}

	results := probeConn(conn, servers)

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败")
		return meta
	}

	fillNATMetadata(meta, results)
	return meta
}

// probeConn 遍历所有 STUN 服务器，收集所有响应端点。
//
// ★ 改动：不再"拿到 2 个样本就 break"，改为"总耗时上限 5 秒"。
//   目的是收集多个不同时刻的端口，供打洞时作为精确候选。
func probeConn(conn net.PacketConn, servers []string) []natProbeResult {
	var results []natProbeResult
	deadline := time.Now().Add(stunProbeTotalTimeout)

	for _, server := range servers {
		if time.Now().After(deadline) {
			log.Printf("[NAT] 总探测时间超 %v，停止（已收集 %d 个端点）",
				stunProbeTotalTimeout, len(results))
			break
		}

		serverAddr, err := net.ResolveUDPAddr("udp4", server)
		if err != nil {
			continue
		}

		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

		if _, err := conn.WriteTo(msg.Raw, serverAddr); err != nil {
			log.Printf("[NAT] STUN 发送 %s 失败: %v", server, err)
			continue
		}

		buf := make([]byte, 1500)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		deadlineConn := time.Now().Add(2 * time.Second)
		for {
			remaining := time.Until(deadlineConn)
			if remaining <= 0 {
				log.Printf("[NAT] STUN %s 超时", server)
				break
			}
			_ = conn.SetReadDeadline(time.Now().Add(remaining))

			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				log.Printf("[NAT] STUN %s 超时", server)
				break
			}
			if from != nil && !sameUDPAddr(from, serverAddr) {
				continue
			}

			resp := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
			if err := resp.Decode(); err != nil {
				continue
			}
			if resp.TransactionID != msg.TransactionID {
				continue
			}

			var xorAddr stun.XORMappedAddress
			if err := xorAddr.GetFrom(resp); err != nil {
				continue
			}
			ip4 := xorAddr.IP.To4()
			if ip4 == nil {
				continue
			}

			results = append(results, natProbeResult{ip: ip4.String(), port: xorAddr.Port})
			log.Printf("[NAT] STUN %s → %s:%d", server, ip4.String(), xorAddr.Port)
			break
		}
	}

	return results
}

func sameUDPAddr(a, b net.Addr) bool {
	ua, ok1 := a.(*net.UDPAddr)
	ub, ok2 := b.(*net.UDPAddr)
	if !ok1 || !ok2 {
		return true
	}
	return ua.IP.Equal(ub.IP) && ua.Port == ub.Port
}

func fillNATMetadata(meta *NATMetadata, results []natProbeResult) {
	meta.PublicEndpoint = net.JoinHostPort(results[0].ip, strconv.Itoa(results[0].port))
	meta.P2PEndpoint = meta.PublicEndpoint

	// ★ 收集所有端点（去重）
	seen := make(map[string]bool)
	for _, r := range results {
		ep := net.JoinHostPort(r.ip, strconv.Itoa(r.port))
		if !seen[ep] {
			seen[ep] = true
			meta.AllEndpoints = append(meta.AllEndpoints, ep)
		}
	}

	firstPort := results[0].port
	allSame := true
	for _, r := range results {
		if r.port != firstPort {
			allSame = false
			break
		}
	}

	if allSame && len(results) >= minStunSamples {
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
		meta.PortsDifference = 0
	} else if len(results) >= minStunSamples {
		meta.NATType = "HardNAT"
		meta.PortsDifference = abs(results[0].port - results[1].port)
		meta.RegularPortsChange = true
	} else {
		meta.NATType = "unknown"
		meta.Behavior = "BehaviorPortChanged"
	}

	log.Printf("[NAT] %s pub=%s endpoints=%d",
		meta.NATType, meta.PublicEndpoint, len(meta.AllEndpoints))
}

func probeNAT(localUDPPort int, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint: "",
		NATType:     "unknown",
		Behavior:    "BehaviorPortChanged",
	}

	servers := stunServers
	if len(servers) == 0 {
		servers = append(servers, stunServersHardcoded...)
		servers = append(servers, stunServersDomain...)
	}

	var results []natProbeResult
	deadline := time.Now().Add(stunProbeTotalTimeout)

	for _, server := range servers {
		if time.Now().After(deadline) {
			break
		}

		c, err := stun.Dial("udp", server)
		if err != nil {
			continue
		}

		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
		var xorAddr stun.XORMappedAddress

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = c.Do(msg, func(res stun.Event) {
				if res.Error != nil {
					return
				}
				_ = xorAddr.GetFrom(res.Message)
			})
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			log.Printf("[NAT] STUN %s 超时", server)
		}
		_ = c.Close()

		if xorAddr.Port == 0 {
			continue
		}
		ip4 := xorAddr.IP.To4()
		if ip4 == nil {
			continue
		}
		results = append(results, natProbeResult{ip: ip4.String(), port: xorAddr.Port})
		log.Printf("[NAT] STUN %s → %s:%d", server, ip4.String(), xorAddr.Port)
	}

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败，NAT 类型未知")
		return meta
	}

	fillNATMetadata(meta, results)
	return meta
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
