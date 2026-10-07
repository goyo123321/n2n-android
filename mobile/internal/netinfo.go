package internal

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type NetInfo struct {
	LANIP     string
	GatewayIP string
}

func getNetInfo() *NetInfo {
	info := &NetInfo{}

	// 优先从路由表拿真实网关
	gw := getDefaultGateway()

	priorities := []string{"wlan0", "wlan1", "eth0"}

	ifaces, err := net.Interfaces()
	if err != nil {
		return info
	}

	for _, want := range priorities {
		for _, iface := range ifaces {
			if iface.Name != want || iface.Flags&net.FlagUp == 0 {
				continue
			}
			if fillNetInfo(info, &iface, gw) {
				log.Printf("[NetInfo] %s LAN=%s GW=%s", iface.Name, info.LANIP, info.GatewayIP)
				return info
			}
		}
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if fillNetInfo(info, &iface, gw) {
			log.Printf("[NetInfo] %s LAN=%s GW=%s (fallback)", iface.Name, info.LANIP, info.GatewayIP)
			return info
		}
	}
	return info
}

func fillNetInfo(info *NetInfo, iface *net.Interface, gateway string) bool {
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
		info.LANIP = ip4.String()

		if gateway != "" {
			info.GatewayIP = gateway
		} else {
			// 兜底：猜 x.x.x.1
			gw := make(net.IP, 4)
			copy(gw, ip4)
			gw[3] = 1
			info.GatewayIP = gw.String()
		}
		return true
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}

// getDefaultGateway 从 /proc/net/route 读取默认路由的网关地址。
//
// 格式（Linux/Android）：
//   Iface   Destination  Gateway   Flags  ...
//   wlan0   00000000     0101A8C0  0003   ...
//
// - Destination = "00000000" 表示默认路由
// - Gateway 是小端 hex，需要按字节对反转后才是网络序
func getDefaultGateway() string {
	f, err := os.Open("/proc/net/route")
	if err != nil {
		return ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// 跳过标题行
	if !scanner.Scan() {
		return ""
	}

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		// fields[0]=Iface, fields[1]=Destination, fields[2]=Gateway
		if fields[1] != "00000000" {
			continue
		}
		if ip := hexLEToIP(fields[2]); ip != nil {
			return ip.String()
		}
	}
	return ""
}

// hexLEToIP 把 /proc/net/route 中的小端 hex 网关地址转成 net.IP
// 例："0101A8C0" → 192.168.1.1
func hexLEToIP(hexStr string) net.IP {
	if len(hexStr) != 8 {
		return nil
	}
	var b [4]byte
	for i := 0; i < 4; i++ {
		v, err := strconv.ParseUint(hexStr[i*2:i*2+2], 16, 8)
		if err != nil {
			return nil
		}
		b[3-i] = byte(v) // 反转字节顺序
	}
	return net.IPv4(b[0], b[1], b[2], b[3])
}

var _ = fmt.Sprintf
