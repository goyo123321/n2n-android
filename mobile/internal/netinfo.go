package internal

import (
	"log"
	"net"
	"os"
	"strings"
)

// getAllLanIPs 收集本机所有物理网卡的私有 IPv4 地址。
//
// 四级 fallback：
//  1. net.Interfaces()（桌面/部分 ROM 正常路径，Android 10+ 常被 SELinux 拒绝）
//  2. 读取 Kotlin 侧写入的 lan_ips.txt
//  3. socket 反推（WS TCP + STUN UDP Dial 的 LocalAddr）
//  4. /proc/net/route 默认路由出口
func getAllLanIPs() []string {
	seen := make(map[string]bool)
	var out []string

	add := func(ip string) {
		if ip == "" || ip == "0.0.0.0" || ip == "::" || ip == "127.0.0.1" {
			return
		}
		if strings.HasPrefix(ip, "10.64.") {
			return
		}
		parsed := net.ParseIP(ip)
		if parsed == nil || !isPrivateIP(parsed) {
			return
		}
		if seen[ip] {
			return
		}
		seen[ip] = true
		out = append(out, ip)
	}

	for _, ip := range getAllLanIPsFromInterfaces() {
		add(ip)
	}
	for _, ip := range readLanIPsFromFile() {
		add(ip)
	}
	for _, ip := range readDefaultRouteIPs() {
		add(ip)
	}

	log.Printf("[NetInfo] 收集到 %d 个局域网 IP: %v", len(out), out)
	return out
}

// collectLanIPsFromSockets 通过已建立的 WS TCP 连接反推本机 LAN IP。
//
// WS TCP LocalAddr 里就是本机真实的网卡出口 IP，不需要 net.Interfaces()。
// 在 WS 连接成功后调用，返回一个或多个（WiFi + 蜂窝）私有 IP。
func collectLanIPsFromSockets(ws *WSTransport) []string {
	seen := make(map[string]bool)
	var out []string

	add := func(ip string) {
		if ip == "" || ip == "0.0.0.0" || ip == "::" {
			return
		}
		if strings.HasPrefix(ip, "10.64.") {
			return
		}
		parsed := net.ParseIP(ip)
		if parsed == nil || !isPrivateIP(parsed) {
			return
		}
		if seen[ip] {
			return
		}
		seen[ip] = true
		out = append(out, ip)
	}

	// WS TCP LocalAddr
	if ws != nil {
		ws.mu.Lock()
		conn := ws.conn
		ws.mu.Unlock()
		if conn != nil {
			if la := conn.LocalAddr(); la != nil {
				if ta, ok := la.(*net.TCPAddr); ok {
					log.Printf("[NetInfo] WS 本地出口: %s", ta.IP.String())
					add(ta.IP.String())
				}
			}
		}
	}

	// UDP Dial 到 STUN 服务器（走物理网卡，因为目标是公网 IP）
	for _, stunServer := range stunServersHardcoded {
		serverAddr, err := net.ResolveUDPAddr("udp4", stunServer)
		if err != nil {
			continue
		}
		conn, err := net.DialUDP("udp4", nil, serverAddr)
		if err != nil {
			continue
		}
		if la := conn.LocalAddr(); la != nil {
			if ua, ok := la.(*net.UDPAddr); ok {
				log.Printf("[NetInfo] STUN Dial 本地出口: %s (→%s)", ua.IP.String(), stunServer)
				add(ua.IP.String())
			}
		}
		_ = conn.Close()
	}

	return out
}

func getAllLanIPsFromInterfaces() []string {
	var out []string

	priorities := []string{"wlan0", "wlan1", "eth0", "eth1"}

	ifaces, err := net.Interfaces()
	if err != nil {
		log.Printf("[NetInfo] 枚举网卡失败: %v", err)
		return out
	}

	seenIP := make(map[string]bool)
	addedIface := make(map[string]bool)

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
			if ip4[0] == 10 && ip4[1] == 64 {
				continue
			}
			if !isPrivateIP(ip4) {
				continue
			}
			ipStr := ip4.String()
			if seenIP[ipStr] {
				continue
			}
			seenIP[ipStr] = true
			out = append(out, ipStr)
			log.Printf("[NetInfo] 网卡 %s → %s", iface.Name, ipStr)
		}
	}

	for _, want := range priorities {
		for i := range ifaces {
			iface := &ifaces[i]
			if iface.Name != want {
				continue
			}
			if !ifaceUsable(iface) {
				continue
			}
			addFromIface(iface)
			addedIface[iface.Name] = true
		}
	}

	for i := range ifaces {
		iface := &ifaces[i]
		if addedIface[iface.Name] {
			continue
		}
		if !ifaceUsable(iface) {
			continue
		}
		if isVirtualIface(iface.Name) {
			continue
		}
		addFromIface(iface)
		addedIface[iface.Name] = true
	}

	log.Printf("[NetInfo] net.Interfaces 收集到 %d 个局域网 IP: %v", len(out), out)
	return out
}

// readLanIPsFromFile 从 Kotlin 侧写入的文件读取局域网 IP。
func readLanIPsFromFile() []string {
	candidates := []string{
		"/data/user/0/com.n2n.android/files/lan_ips.txt",
		"/data/data/com.n2n.android/files/lan_ips.txt",
	}

	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var out []string
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			ip := net.ParseIP(line)
			if ip == nil {
				continue
			}
			ip4 := ip.To4()
			if ip4 == nil {
				continue
			}
			if ip4[0] == 10 && ip4[1] == 64 {
				continue
			}
			if !isPrivateIP(ip4) {
				continue
			}
			out = append(out, ip4.String())
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// readDefaultRouteIPs 从 /proc/net/route 读默认路由接口的 IPv4。
func readDefaultRouteIPs() []string {
	data, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return nil
	}
	var ifaces []string
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if i == 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[1] != "00000000" {
			continue
		}
		ifaces = append(ifaces, fields[0])
	}

	var out []string
	for _, ifaceName := range ifaces {
		iface, err := net.InterfaceByName(ifaceName)
		if err != nil {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			out = append(out, ip4.String())
		}
	}
	return out
}

func ifaceUsable(iface *net.Interface) bool {
	if iface.Flags&net.FlagLoopback != 0 {
		return false
	}
	if iface.Flags&net.FlagUp != 0 {
		return true
	}
	addrs, err := iface.Addrs()
	if err != nil || len(addrs) == 0 {
		return false
	}
	return true
}

// isVirtualIface 判断是否是虚拟/隧道接口。
// 注意：rmnet* 是蜂窝数据接口，不过滤。
func isVirtualIface(name string) bool {
	if name == "n2n0" {
		return true
	}
	for _, prefix := range []string{"tun", "tap", "utun", "ppp", "p2p"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	return ip4[0] == 10 ||
		(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31) ||
		(ip4[0] == 192 && ip4[1] == 168)
}
