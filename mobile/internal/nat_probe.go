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
	NATType            string
	PortsDifference    int
	RegularPortsChange bool
	Behavior           string
	AssistedSockets    []string
	MultiExit          bool // ★ 新增
}

// ★ 硬编码 STUN IP：不依赖 DNS，纯 VPN 场景也能工作。
//
// 这些都是长期稳定的 anycast IP：
//   - 74.125.250.129 / 74.125.204.127 → stun.l.google.com (anycast)
//   - 162.159.207.0                   → stun.cloudflare.com
var stunServersHardcoded = []string{
	"74.125.250.129:19302",
	"74.125.204.127:19302",
	"162.159.207.0:3478",
}

// ★ 域名兜底：IP 全部失败后才尝试（需 DNS 可用）
var stunServersDomain = []string{
	"stun.l.google.com:19302",
	"stun.cloudflare.com:3478",
	"stun.miwifi.com:3478",
	"stun.chat.bilibili.com:3478",
	"stun.hitv.com:3478",
}

// 至少需要几个成功样本才能判断 NAT 类型
const minStunSamples = 2

type natProbeResult struct {
	ip   string
	port int
}

// probeNATWithConn 用已有的 PacketConn（protected）做 STUN 探测
// 不关闭 conn，由调用方管理生命周期
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

	// 取本地端口，用于 localLANAddrs
	if la := conn.LocalAddr(); la != nil {
		if ua, ok := la.(*net.UDPAddr); ok {
			meta.AssistedSockets = localLANAddrs(ua.Port)
		}
	}

	// 决定要探测的 server 列表：
	// - 外部传入优先
	// - 否则 IP 列表 → 域名列表
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

// probeConn 遍历 servers，收集成功结果。
// 在拿到 minStunSamples 个样本后提前退出，避免每次都等满超时。
func probeConn(conn net.PacketConn, servers []string) []natProbeResult {
	var results []natProbeResult

	for _, server := range servers {
		serverAddr, err := net.ResolveUDPAddr("udp4", server)
		if err != nil {
			// DNS 失败或 IP 非法，直接跳过
			continue
		}

		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

		if _, err := conn.WriteTo(msg.Raw, serverAddr); err != nil {
			log.Printf("[NAT] STUN 发送 %s 失败: %v", server, err)
			continue
		}

		buf := make([]byte, 1500)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		// ★ 循环读，直到拿到属于本次事务的响应或超时
		//   （防止收到其它 socket 的陈旧数据）
		deadline := time.Now().Add(2 * time.Second)
		for {
			remaining := time.Until(deadline)
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
			// 只接受来自目标服务器的响应
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

		// 拿到足够样本就提前退出
		if len(results) >= minStunSamples {
			break
		}
	}

	return results
}

func sameUDPAddr(a, b net.Addr) bool {
	ua, ok1 := a.(*net.UDPAddr)
	ub, ok2 := b.(*net.UDPAddr)
	if !ok1 || !ok2 {
		return true // 拿不到强类型就不校验
	}
	return ua.IP.Equal(ub.IP) && ua.Port == ub.Port
}

func fillNATMetadata(meta *NATMetadata, results []natProbeResult) {
	meta.PublicEndpoint = net.JoinHostPort(results[0].ip, strconv.Itoa(results[0].port))
	meta.P2PEndpoint = meta.PublicEndpoint

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
		// 只拿到一个样本，保守判为 EasyNAT（后续用 p2p 打洞结果学习）
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
	}

	log.Printf("[NAT] %s pub=%s", meta.NATType, meta.PublicEndpoint)
}

// probeNAT 原方法（非 protected socket，回退用）
func probeNAT(localUDPPort int, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint:     "",
		NATType:         "unknown",
		Behavior:        "BehaviorPortChanged",
		AssistedSockets: localLANAddrs(localUDPPort),
	}

	servers := stunServers
	if len(servers) == 0 {
		servers = append(servers, stunServersHardcoded...)
		servers = append(servers, stunServersDomain...)
	}

	var results []natProbeResult

	for _, server := range servers {
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

		if len(results) >= minStunSamples {
			break
		}
	}

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败，NAT 类型未知")
		return meta
	}

	fillNATMetadata(meta, results)
	return meta
}

func localLANAddrs(port int) []string {
	var out []string
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			out = append(out, net.JoinHostPort(ip4.String(), strconv.Itoa(port)))
		}
	}
	return out
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
