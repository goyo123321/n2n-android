package internal

import (
	"bufio"
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
		// 跳过 n2n 虚拟网段 10.64.0.0/24
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

// sameSubnet 判断两个 IPv4 是否在同一 /24 子网。
//
// 假设 /24 覆盖了绝大多数家庭 / 办公室 WiFi 场景。
// 更严格的做法是拿到本机子网掩码精确判断，但收益很小。
func sameSubnet(a, b string) bool {
	ipA := net.ParseIP(a).To4()
	ipB := net.ParseIP(b).To4()
	if ipA == nil || ipB == nil {
		return false
	}
	return ipA[0] == ipB[0] && ipA[1] == ipB[1] && ipA[2] == ipB[2]
}

func getDefaultGateway() string {
	f, err := os.Open("/proc/net/route")
	if err != nil {
		return ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return ""
	}

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		if fields[1] != "00000000" {
			continue
		}
		if ip := hexLEToIP(fields[2]); ip != nil {
			return ip.String()
		}
	}
	return ""
}

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
		b[3-i] = byte(v)
	}
	return net.IPv4(b[0], b[1], b[2], b[3])
}
