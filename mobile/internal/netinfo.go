package internal

import (
	"log"
	"net"
)

type NetInfo struct {
	LANIP     string
	GatewayIP string
}

func getNetInfo() *NetInfo {
	info := &NetInfo{}
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
			if fillNetInfo(info, &iface) {
				log.Printf("[NetInfo] %s LAN=%s GW=%s", iface.Name, info.LANIP, info.GatewayIP)
				return info
			}
		}
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if fillNetInfo(info, &iface) {
			log.Printf("[NetInfo] %s LAN=%s GW=%s (fallback)", iface.Name, info.LANIP, info.GatewayIP)
			return info
		}
	}
	return info
}

func fillNetInfo(info *NetInfo, iface *net.Interface) bool {
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
		info.LANIP = ip4.String()
		gw := make(net.IP, 4)
		copy(gw, ip4)
		gw[3] = 1
		info.GatewayIP = gw.String()
		return true
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}
