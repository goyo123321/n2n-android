package internal

import (
	"log"
	"net"
	"strings"
)

// getAllLanIPs 收集本机所有物理网卡的私有 IPv4 地址。
//
// 返回顺序：优先 wlan0/eth0，其次是其他接口。
// 跳过：
//   - 回环 / 未启用接口
//   - VPN / 虚拟网卡（tun* / tap* / utun* / ppp* / n2n0）
//   - n2n 虚拟网段 10.64.0.0/24
//   - 非私有 IP（公网 IP 不在此列）
//
// 不保证顺序，也不做去重——客户端会把整个列表上报，服务端根据
// 是否与其他 peer 有交集自行决定尝试哪个。
func getAllLanIPs() []string {
	var out []string

	// 优先级：先 wlan0，再 eth0，最后其他
	priorities := []string{"wlan0", "wlan1", "eth0"}

	ifaces, err := net.Interfaces()
	if err != nil {
		log.Printf("[NetInfo] 枚举网卡失败: %v", err)
		return out
	}

	seen := make(map[string]bool)
	addFromIface := func(iface *net.Interface) {
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
			// 跳过 n2n 虚拟网段
			if ip4[0] == 10 && ip4[1] == 64 {
				continue
			}
			if !isPrivateIP(ip4) {
				continue
			}
			ipStr := ip4.String()
			if seen[ipStr] {
				continue
			}
			seen[ipStr] = true
			out = append(out, ipStr)
		}
	}

	// 高优先级接口先加
	for _, want := range priorities {
		for _, iface := range ifaces {
			if iface.Name != want || iface.Flags&net.FlagUp == 0 {
				continue
			}
			addFromIface(&iface)
		}
	}

	// 其余接口补上
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if isVirtualIface(iface.Name) {
			continue
		}
		addFromIface(&iface)
	}

	log.Printf("[NetInfo] 收集到 %d 个局域网 IP: %v", len(out), out)
	return out
}

func isVirtualIface(name string) bool {
	if name == "n2n0" {
		return true
	}
	for _, prefix := range []string{"tun", "tap", "utun", "ppp", "p2p", "rmnet"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}
