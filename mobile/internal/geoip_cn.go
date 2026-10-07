package internal

import (
	_ "embed"
	"log"
	"net"
	"strings"
)

//go:embed data/geoip_cn.txt
var geoipCNData string

var geoipCNNets []*net.IPNet

func init() {
	lines := strings.Split(geoipCNData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		_, n, err := net.ParseCIDR(line)
		if err == nil {
			geoipCNNets = append(geoipCNNets, n)
		}
	}
	log.Printf("[GeoIP] 加载 %d 条国内 IP 段", len(geoipCNNets))
}

func IsChinaIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, n := range geoipCNNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
