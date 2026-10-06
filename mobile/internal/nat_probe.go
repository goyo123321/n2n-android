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
}

// probeNATWithConn 用已有的 PacketConn（protected）做 STUN 探测
// 不关闭 conn，由调用方管理生命周期
func probeNATWithConn(conn net.PacketConn, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint:     "",
		NATType:         "unknown",
		Behavior:        "BehaviorPortChanged",
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

	if len(stunServers) == 0 {
		stunServers = []string{
			"stun.l.google.com:19302",
			"stun.miwifi.com:3478",
			"stun.chat.bilibili.com:3478",
			"stun.hitv.com:3478",
			"stun.cloudflare.com:3478",
		}
	}

	type result struct {
		ip   string
		port int
	}
	var results []result

	for _, server := range stunServers {
		serverAddr, err := net.ResolveUDPAddr("udp4", server)
		if err != nil {
			log.Printf("[NAT] 解析 %s 失败: %v", server, err)
			continue
		}

		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

		if _, err := conn.WriteTo(msg.Raw, serverAddr); err != nil {
			log.Printf("[NAT] STUN 发送 %s 失败: %v", server, err)
			continue
		}

		buf := make([]byte, 1500)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("[NAT] STUN %s 超时", server)
			continue
		}

		resp := &stun.Message{Raw: buf[:n]}
		if err := resp.Decode(); err != nil {
			log.Printf("[NAT] STUN 解析 %s 失败: %v", server, err)
			continue
		}

		var xorAddr stun.XORMappedAddress
		if err := xorAddr.GetFrom(resp); err != nil {
			log.Printf("[NAT] STUN 提取地址 %s 失败: %v", server, err)
			continue
		}

		ip4 := xorAddr.IP.To4()
		if ip4 == nil {
			continue
		}

		results = append(results, result{ip: ip4.String(), port: xorAddr.Port})
		log.Printf("[NAT] STUN %s → %s:%d", server, ip4.String(), xorAddr.Port)
	}

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败")
		return meta
	}

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

	if allSame && len(results) >= 2 {
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
	} else if len(results) >= 2 {
		meta.NATType = "HardNAT"
		meta.PortsDifference = abs(results[0].port - results[1].port)
		meta.RegularPortsChange = true
	} else {
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
	}

	log.Printf("[NAT] %s pub=%s", meta.NATType, meta.PublicEndpoint)
	return meta
}

// probeNAT 原方法（非 protected socket，回退用）
func probeNAT(localUDPPort int, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint:     "",
		NATType:         "unknown",
		Behavior:        "BehaviorPortChanged",
		AssistedSockets: localLANAddrs(localUDPPort),
	}

	if len(stunServers) == 0 {
		stunServers = []string{"stun.l.google.com:19302", "stun.cloudflare.com:3478"}
	}

	type result struct {
		ip   string
		port int
	}
	var results []result

	for _, server := range stunServers {
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
		case <-time.After(3 * time.Second):
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
		results = append(results, result{ip: ip4.String(), port: xorAddr.Port})
		log.Printf("[NAT] STUN %s → %s:%d", server, ip4.String(), xorAddr.Port)
	}

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败，NAT 类型未知")
		return meta
	}

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

	if allSame && len(results) >= 2 {
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
		meta.PortsDifference = 0
	} else if len(results) >= 2 {
		meta.NATType = "HardNAT"
		meta.PortsDifference = abs(results[0].port - results[1].port)
		meta.RegularPortsChange = true
	} else {
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
	}

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
