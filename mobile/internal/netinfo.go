package internal

import "net"

// isPrivateIP 判断是否为私有 IPv4 地址。
//
// 保留这个函数是因为清理 LAN 收集时，某些老版本代码路径可能还引用。
// 最小化实现，不做任何 I/O。
func isPrivateIP(ip net.IP) bool {
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	return ip4[0] == 10 ||
		(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31) ||
		(ip4[0] == 192 && ip4[1] == 168)
}
