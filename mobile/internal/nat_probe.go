package internal

import (
	"log"
	"net"
	"strconv"
	"strings"
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

	if la := conn.LocalAddr(); la != nil {
		if ua, ok := la.(*net.UDPAddr); ok {
			meta.AssistedSockets = localLANAddrs(ua.Port)
		}
	}

	defer func() {
		if la := conn.LocalAddr(); la != nil {
			if ua, ok := la.(*net.UDPAddr); ok {
				ip := ua.IP.String()
				if ip != "0.0.0.0" && ip != "" && ip != "::" {
					existing := false
					portSuffix := ":" + strconv.Itoa(ua.Port)
					for _, s := range meta.AssistedSockets {
						if strings.HasSuffix(s, portSuffix) && strings.HasPrefix(s, ip+":") {
							existing = true
							break
						}
					}
					if !existing {
						meta.AssistedSockets = append(meta.AssistedSockets,
							net.JoinHostPort(ip, strconv.Itoa(ua.Port)))
						log.Printf("[NAT] socket 探测后本地出口: %s", ip)
					}
				}
			}
		}
	}()

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

func probeConn(conn net.PacketConn, servers []string) []natProbeResult {
	var results []natProbeResult

	for _, server := range servers {
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
		return true
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
		// ★ 只拿到 1 个样本 → unknown，不猜 EasyNAT
		//   猜错会让服务端按错误模式派发指令，且两端判定不一致
		meta.NATType = "unknown"
		meta.Behavior = "BehaviorPortChanged"
	}

	log.Printf("[NAT] %s pub=%s", meta.NATType, meta.PublicEndpoint)
}

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
