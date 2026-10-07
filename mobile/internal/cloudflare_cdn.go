package internal

import (
	"log"
	"net"
)

var cfCDNRanges = []string{
	"103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
	"104.16.0.0/13", "104.24.0.0/14", "108.162.192.0/18",
	"131.0.72.0/22", "141.101.64.0/18", "162.158.0.0/15",
	"172.64.0.0/13", "173.245.48.0/20", "188.114.96.0/20",
	"190.93.240.0/20", "197.234.240.0/22", "198.41.128.0/17",
}

var cfCDNNets []*net.IPNet

func init() {
	for _, s := range cfCDNRanges {
		_, n, err := net.ParseCIDR(s)
		if err == nil {
			cfCDNNets = append(cfCDNNets, n)
		}
	}
	log.Printf("[CF-CDN] 加载 %d 条 CF CDN IP 段", len(cfCDNNets))
}

func IsCloudflareCDN(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, n := range cfCDNNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
