package internal

import (
	"log"
	"net"
	"strings"
)

// NetInfo 本机网络信息。
//
// 收集本机所有可用的局域网 IPv4——包括 WiFi / 蜂窝 / 以太网。
// 手机可能同时有多个出口（WiFi + 4G），全部上报，
// 让对端能匹配到任意一个。
type NetInfo struct {
	LANIPs []string
}

// vpnIfacePrefixes 要排除的接口名前缀。
//
// 这些接口都不是物理网络出口：
//   - tun/tap/ppp/utun: 其他 VPN 客户端
//   - ipsec: 系统 IPSec
//   - n2n: 我们自己的 TUN（绝对要排除，否则会递归）
//   - wg: WireGuard
var vpnIfacePrefixes = []string{
	"tun", "tap", "ppp", "utun", "ipsec", "n2n", "wg",
}

func getNetInfo() *NetInfo {
	info := &NetInfo{}

	ifaces, err := net.Interfaces()
	if err != nil {
		log.Printf("[NetInfo] 枚举接口失败: %v", err)
		return info
	}

	for _, iface := range ifaces {
		// 跳过 loopback 和未启动
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// 跳过 VPN 接口
		if isVPNInterface(iface.Name) {
			log.Printf("[NetInfo] 跳过 VPN 接口: %s", iface.Name)
			continue
		}
		// 收集该接口的所有私有 IPv4
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
			// 跳过我们的虚拟网段 10.64.0.0/24
			if ip4[0] == 10 && ip4[1] == 64 {
				continue
			}
			if !isPrivateIP(ip4) {
				continue
			}
			ipStr := ip4.String()
			if !containsString(info.LANIPs, ipStr) {
				info.LANIPs = append(info.LANIPs, ipStr)
			}
		}
	}

	log.Printf("[NetInfo] 可用局域网出口: %v", info.LANIPs)
	return info
}

func isVPNInterface(name string) bool {
	lower := strings.ToLower(name)
	for _, prefix := range vpnIfacePrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	if len(ip) < 4 {
		return false
	}
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// sameSubnet 判断两个 IPv4 是否在同一 /24 子网。
func sameSubnet(a, b string) bool {
	ipA := net.ParseIP(a).To4()
	ipB := net.ParseIP(b).To4()
	if ipA == nil || ipB == nil {
		return false
	}
	return ipA[0] == ipB[0] && ipA[1] == ipB[1] && ipA[2] == ipB[2]
}

// anySameSubnet 判断 mine 列表里是否有 IP 与 other 同子网。
func anySameSubnet(mine []string, other string) bool {
	for _, m := range mine {
		if sameSubnet(m, other) {
			return true
		}
	}
	return false
}
