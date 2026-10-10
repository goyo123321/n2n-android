package internal

import (
	"encoding/binary"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/pion/stun/v2"
)

type NATMetadata struct {
	P2PEndpoint        string
	PublicEndpoint     string
	AllEndpoints       []string // ★ 所有 STUN 结果（多时刻）
	NATType            string
	PortsDifference    int
	RegularPortsChange bool
	Behavior           string
	MultiExit          bool
}

var stunServersHardcoded = []string{
	"74.125.250.129:19302",
	"74.125.204.127:19302",
	"162.159.207.0:3478",
}

var stunServersDomain = []string{
	"stun.l.google.com:19302",
	"stun.cloudflare.com:3478",
	"stun.miwifi.com:3478",
	"stun.chat.bilibili.com:3478",
	"stun.hitv.com:3478",
}

const minStunSamples = 2

type natProbeResult struct {
	ip   string
	port int
}

// parseSTUNResponse 解析 STUN Binding Response 里的 XOR-MAPPED-ADDRESS。
//
// 输入：udpConn 收到的任意 UDP 包
// 输出：(publicIP, publicPort, ok)
//
// 只识别 msgType=0x0101（Binding Success Response），其他一律返回 false。
func parseSTUNResponse(data []byte) (string, int, bool) {
	if len(data) < 20 {
		return "", 0, false
	}
	// Binding Success Response: msgType = 0x0101
	if data[0] != 0x01 || data[1] != 0x01 {
		return "", 0, false
	}
	// magic cookie 校验
	if binary.BigEndian.Uint32(data[4:8]) != 0x2112A442 {
		return "", 0, false
	}

	msgLen := int(binary.BigEndian.Uint16(data[2:4]))
	end := 20 + msgLen
	if end > len(data) {
		end = len(data)
	}

	offset := 20
	for offset+4 <= end {
		typ := binary.BigEndian.Uint16(data[offset : offset+2])
		l := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		if offset+4+l > end {
			break
		}
		// XOR-MAPPED-ADDRESS = 0x0020
		if typ == 0x0020 {
			ip, port, err := xorDecodePeer(data[offset+4 : offset+4+l])
			if err == nil && ip.To4() != nil {
				return ip.To4().String(), port, true
			}
		}
		offset += 4 + ((l + 3) &^ 3)
	}
	return "", 0, false
}

func probeNATWithConn(conn net.PacketConn, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint: "",
		NATType:    "unknown",
		Behavior:   "BehaviorPortChanged",
	}

	if conn == nil {
		log.Printf("[NAT] 无 protected socket，回退默认方法")
		return probeNAT(0, stunServers)
	}

	servers := stunServers
	if len(servers) == 0 {
		servers = append(servers, stunServersHardcoded...)
		servers = append(servers, stunServersDomain...)
	}

	results := probeConn(conn, servers)

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败")
		return meta
	}

	fillNATMetadata(meta, results)
	return meta
}

func probeConn(conn net.PacketConn, servers []string) []natProbeResult {
	var results []natProbeResult

	for _, server := range servers {
		serverAddr, err := net.ResolveUDPAddr("udp4", server)
		if err != nil {
			continue
		}

		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

		if _, err := conn.WriteTo(msg.Raw, serverAddr); err != nil {
			log.Printf("[NAT] STUN 发送 %s 失败: %v", server, err)
			continue
		}

		buf := make([]byte, 1500)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		deadline := time.Now().Add(2 * time.Second)
		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				log.Printf("[NAT] STUN %s 超时", server)
				break
			}
			_ = conn.SetReadDeadline(time.Now().Add(remaining))

			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				log.Printf("[NAT] STUN %s 超时", server)
				break
			}
			if from != nil && !sameUDPAddr(from, serverAddr) {
				continue
			}

			resp := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
			if err := resp.Decode(); err != nil {
				continue
			}
			if resp.TransactionID != msg.TransactionID {
				continue
			}

			var xorAddr stun.XORMappedAddress
			if err := xorAddr.GetFrom(resp); err != nil {
				continue
			}
			ip4 := xorAddr.IP.To4()
			if ip4 == nil {
				continue
			}

			results = append(results, natProbeResult{ip: ip4.String(), port: xorAddr.Port})
			log.Printf("[NAT] STUN %s → %s:%d", server, ip4.String(), xorAddr.Port)
			break
		}

		if len(results) >= minStunSamples {
			break
		}
	}

	return results
}

func sameUDPAddr(a, b net.Addr) bool {
	ua, ok1 := a.(*net.UDPAddr)
	ub, ok2 := b.(*net.UDPAddr)
	if !ok1 || !ok2 {
		return true
	}
	return ua.IP.Equal(ub.IP) && ua.Port == ub.Port
}

func fillNATMetadata(meta *NATMetadata, results []natProbeResult) {
	meta.PublicEndpoint = net.JoinHostPort(results[0].ip, strconv.Itoa(results[0].port))
	meta.P2PEndpoint = meta.PublicEndpoint

	// ★ 收集所有端点（去重）
	seen := make(map[string]bool)
	for _, r := range results {
		ep := net.JoinHostPort(r.ip, strconv.Itoa(r.port))
		if !seen[ep] {
			seen[ep] = true
			meta.AllEndpoints = append(meta.AllEndpoints, ep)
		}
	}

	firstPort := results[0].port
	allSame := true
	for _, r := range results {
		if r.port != firstPort {
			allSame = false
			break
		}
	}

	if allSame && len(results) >= minStunSamples {
		meta.NATType = "EasyNAT"
		meta.Behavior = "BehaviorNoChange"
		meta.PortsDifference = 0
	} else if len(results) >= minStunSamples {
		meta.NATType = "HardNAT"
		meta.PortsDifference = abs(results[0].port - results[1].port)
		meta.RegularPortsChange = true
	} else {
		meta.NATType = "unknown"
		meta.Behavior = "BehaviorPortChanged"
	}

	log.Printf("[NAT] %s pub=%s endpoints=%d", meta.NATType, meta.PublicEndpoint, len(meta.AllEndpoints))
}

func probeNAT(localUDPPort int, stunServers []string) *NATMetadata {
	meta := &NATMetadata{
		P2PEndpoint: "",
		NATType:     "unknown",
		Behavior:    "BehaviorPortChanged",
	}

	servers := stunServers
	if len(servers) == 0 {
		servers = append(servers, stunServersHardcoded...)
		servers = append(servers, stunServersDomain...)
	}

	var results []natProbeResult

	for _, server := range servers {
		c, err := stun.Dial("udp", server)
		if err != nil {
			continue
		}

		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
		var xorAddr stun.XORMappedAddress

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = c.Do(msg, func(res stun.Event) {
				if res.Error != nil {
					return
				}
				_ = xorAddr.GetFrom(res.Message)
			})
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			log.Printf("[NAT] STUN %s 超时", server)
		}
		_ = c.Close()

		if xorAddr.Port == 0 {
			continue
		}
		ip4 := xorAddr.IP.To4()
		if ip4 == nil {
			continue
		}
		results = append(results, natProbeResult{ip: ip4.String(), port: xorAddr.Port})
		log.Printf("[NAT] STUN %s → %s:%d", server, ip4.String(), xorAddr.Port)

		if len(results) >= minStunSamples {
			break
		}
	}

	if len(results) == 0 {
		log.Printf("[NAT] 所有 STUN 探测失败，NAT 类型未知")
		return meta
	}

	fillNATMetadata(meta, results)
	return meta
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
