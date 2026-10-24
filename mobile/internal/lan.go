package internal

// Android 端 LAN 地址收集。
//
// 与 PC 端差异：
//   1. 无法列出 net.Interfaces()——部分 ROM 权限限制，且 tun 接口会污染结果
//   2. 主要依赖 UDP connect 骗内核返回出口 IP
//   3. tun 接口（tun0 / n2n0 等）必须过滤掉

import (
	"net"
	"sort"
	"strconv"
	"strings"
)

// tun 接口名前缀——这些接口是 VPN 自己建的，不是真正的 LAN
var tunInterfacePrefixes = []string{
	"tun", "tap", "n2n", "utun", "ppp", "wg", "ipsec",
}

func isTunInterface(name string) bool {
	lname := strings.ToLower(name)
	for _, prefix := range tunInterfacePrefixes {
		if strings.HasPrefix(lname, prefix) {
			return true
		}
	}
	return false
}

// collectLANEndpoints 收集本机所有 LAN 端点。
//
// 双路径：
//   1. UDP connect 骗内核返回出口 IP —— Android 最可靠
//   2. 兜底遍历 net.Interfaces() —— 覆盖多网卡，但需过滤 tun
func collectLANEndpoints(port int, selfVIP string) []string {
	var result []string
	seen := make(map[string]bool)

	add := func(ip4 net.IP) {
		if ip4 == nil {
			return
		}
		if selfVIP != "" && ip4.String() == selfVIP {
			return
		}
		if isCGNATOverlay(ip4) {
			return
		}
		if !isPrivateIPv4(ip4) {
			return
		}
		if ip4.IsLoopback() || ip4.IsLinkLocalUnicast() {
			return
		}
		ep := net.JoinHostPort(ip4.String(), strconv.Itoa(port))
		if seen[ep] {
			return
		}
		seen[ep] = true
		result = append(result, ep)
	}

	// 路径 1：UDP connect 骗内核
	if ip := detectOutboundIP(); ip != nil {
		add(ip)
	}

	// 路径 2：遍历接口（过滤 tun）
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			if isTunInterface(iface.Name) {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if ip == nil || ip.To4() == nil {
					continue
				}
				add(ip.To4())
			}
		}
	}

	return result
}

// detectOutboundIP 用一个不真正发包的 UDP connect 骗内核选出出口网卡。
//
// 原理：connect UDP 只更新内核 socket 的路由缓存，不触发 ARP、不发包。
// getsockname() 返回内核为该 socket 选定的本地 IP —— 即"出网卡"的 IP。
//
// ★ Android 特有：即使 VPN 已经启动，只要 socket 未 protect，
//   内核仍会按默认路由选出口。因为 Go 侧探测用的都是 protected socket，
//   所以这里拿到的必定是物理网络的出口 IP。
func detectOutboundIP() net.IP {
	targets := []string{
		"8.8.8.8:80",
		"1.1.1.1:80",
		"114.114.114.114:80",
	}
	for _, t := range targets {
		conn, err := net.Dial("udp4", t)
		if err != nil {
			continue
		}
		localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
		_ = conn.Close()
		if !ok || localAddr == nil || localAddr.IP == nil {
			continue
		}
		ip4 := localAddr.IP.To4()
		if ip4 != nil {
			return ip4
		}
	}
	return nil
}

func isPrivateIPv4(ip net.IP) bool {
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	if ip4[0] == 10 {
		return true
	}
	if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
		return true
	}
	if ip4[0] == 192 && ip4[1] == 168 {
		return true
	}
	return false
}

func isCGNATOverlay(ip net.IP) bool {
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	return ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127
}

// localPrefixes 返回本机所有可能是 LAN 前缀的网段，用于 LAN 亲和度排序
// 和 lanSubnets 上报。
//
// ★ 修复：Android 上 net.Interfaces() 经常返回空/受限，
//   用 detectOutboundIP（UDP connect 骗内核）兜底推断 /24。
//   否则：
//   - shouldShareLan 返回 false → 同 LAN 走公网打洞
//   - 双 WAN 场景 lanSubnets 为空 → 无法用 cidrOverlaps 判断
//   - A 换网络后 lanSubnets 为空 → 服务端不识别同 LAN
//   - sendICMPDirect 的 selfOnLAN 判断为 false → ICMP 走公网
func localPrefixes(selfVIP string) []*net.IPNet {
	var result []*net.IPNet
	seen := map[string]bool{}

	add := func(ipnet *net.IPNet) {
		if ipnet == nil || ipnet.IP == nil {
			return
		}
		ip4 := ipnet.IP.To4()
		if ip4 == nil {
			return
		}
		if isCGNATOverlay(ip4) {
			return
		}
		if selfVIP != "" && ip4.String() == selfVIP {
			return
		}
		if !isPrivateIPv4(ip4) {
			return
		}
		if ip4.IsLoopback() || ip4.IsLinkLocalUnicast() {
			return
		}
		key := ipnet.String()
		if seen[key] {
			return
		}
		seen[key] = true
		result = append(result, ipnet)
	}

	// 路径 1：net.Interfaces()
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			if isTunInterface(iface.Name) {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					add(ipnet)
				}
			}
		}
	}

	// ★ 路径 2：detectOutboundIP 兜底
	//
	// Android 上 net.Interfaces() 可能返回空/受限，用 UDP connect 骗内核
	// 拿到出口 IP，推断 /24 网段。
	if outIP := detectOutboundIP(); outIP != nil {
		mask := net.CIDRMask(24, 32)
		outNet := &net.IPNet{IP: outIP.Mask(mask), Mask: mask}
		add(outNet)
	}

	return result
}

// sortLANByAffinity 按本机前缀亲和度排序 LAN 端点。
//
// 分数 = 匹配的前缀中最大 mask 位数（越精确越优先）。
func sortLANByAffinity(lan []string, prefixes []*net.IPNet) []string {
	if len(lan) < 2 || len(prefixes) == 0 {
		return lan
	}
	type entry struct {
		ep    string
		score int
	}
	scored := make([]entry, len(lan))
	for i, ep := range lan {
		host, _, err := net.SplitHostPort(ep)
		if err != nil {
			host = ep
		}
		ip := net.ParseIP(host)
		score := 0
		if ip != nil && !isCGNATOverlay(ip) {
			for _, p := range prefixes {
				if p.Contains(ip) {
					ones, _ := p.Mask.Size()
					if ones > score {
						score = ones
					}
				}
			}
		}
		scored[i] = entry{ep, score}
	}
	sort.SliceStable(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})
	out := make([]string, len(scored))
	for i, s := range scored {
		out[i] = s.ep
	}
	return out
}
