package internal

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DNSCache struct {
	mu      sync.RWMutex
	ipToDom map[string]string
	ttl     map[string]time.Time
}

func NewDNSCache() *DNSCache {
	return &DNSCache{ipToDom: make(map[string]string), ttl: make(map[string]time.Time)}
}

func (c *DNSCache) LookupDomainByIP(ip string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if exp, ok := c.ttl["ip:"+ip]; ok && time.Now().Before(exp) {
		return c.ipToDom[ip]
	}
	return ""
}

func (c *DNSCache) Store(ip, domain string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ipToDom[ip] = domain
	c.ttl["ip:"+ip] = time.Now().Add(ttl)
}

type DNSProxy struct {
	cache         *DNSCache
	httpClientCN  *http.Client
	httpClientOut *http.Client
}

// 国内 DoH：只保留能硬编码 IP 的，避免解析 DoH 域名本身触发死循环
var dohServersCN = []string{
	"https://dns.alidns.com/dns-query",
	"https://doh.pub/dns-query",
}

var dohServersOut = []string{
	"https://cloudflare-dns.com/dns-query",
	"https://dns.google/dns-query",
}

var dohOutIPs = []string{"1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4"}

// ★ 硬编码 DoH 服务器 IP，避免 DNS 死循环
//
// 死循环场景：
//   系统 DNS 指向 TUN 虚拟 IP → 查询进 netstack → DNSProxy 用 DoH
//   → DoH 服务器域名（如 dns.alidns.com）需要解析 → 走 Go resolver
//   → 读系统 DNS（还是 TUN 虚拟 IP）→ 又回到 netstack → 无限循环
//
// 解决：DialContext 里查表命中就直接连 IP，TCP 目标不是域名，Go 不会解析
var dohHostIPs = map[string]string{
	"dns.alidns.com": "223.5.5.5",
	"doh.pub":        "1.12.12.12",
}

func NewDNSProxy(cache *DNSCache, getWSOutbound func() *WSOutbound) (*DNSProxy, error) {
	protectedDialer := newProtectedDialer()

	// ★ 国内 DoH：优先走硬编码 IP
	//
	// 说明：
	//   - DialContext 只负责 TCP 层，把域名换成 IP，Go 就不会去解析域名
	//   - TLSClientConfig 保持空 ServerName → Go 会自动用 URL 的 host
	//     （dns.alidns.com 等）作为 SNI，证书验证依然通过
	transportCN := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return protectedDialer.DialContext(ctx, network, addr)
			}
			// 命中映射表 → 直连硬编码 IP
			if targetIP, ok := dohHostIPs[host]; ok {
				return protectedDialer.DialContext(ctx, network, targetIP+":"+port)
			}
			// 兜底：原样走 protectedDialer（可能会尝试解析域名）
			return protectedDialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: false,
	}

	// 国外 DoH：走 Workers 出口
	transportOut := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			wsOut := getWSOutbound()
			if wsOut == nil {
				return nil, fmt.Errorf("wsOutbound 未就绪")
			}
			_, portStr, err := net.SplitHostPort(addr)
			if err != nil {
				portStr = "443"
			}
			port, _ := strconv.Atoi(portStr)
			if port <= 0 {
				port = 443
			}
			var lastErr error
			for _, ip := range dohOutIPs {
				stream, err := wsOut.NewStream(ip, port)
				if err == nil {
					return newStreamConn(stream, addr), nil
				}
				lastErr = err
			}
			return nil, fmt.Errorf("DoH 出站连接失败: %v", lastErr)
		},
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: false,
	}

	return &DNSProxy{
		cache:         cache,
		httpClientCN:  &http.Client{Timeout: 5 * time.Second, Transport: transportCN},
		httpClientOut: &http.Client{Timeout: 8 * time.Second, Transport: transportOut},
	}, nil
}

func (p *DNSProxy) dohQuery(client *http.Client, base, domain string) ([]string, error) {
	url := fmt.Sprintf("%s?name=%s&type=A", base, domain)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result struct {
		Answer []struct {
			Type int    `json:"type"`
			Data string `json:"data"`
		} `json:"Answer"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	var ips []string
	for _, a := range result.Answer {
		if a.Type == 1 && net.ParseIP(a.Data) != nil {
			ips = append(ips, a.Data)
		}
	}
	return ips, nil
}

func (p *DNSProxy) ResolveViaDoH(domain string) ([]string, error) {
	isCN := IsChinaDomain(domain)
	var primary, fallback []string
	var primaryClient, fallbackClient *http.Client

	if isCN {
		primary, primaryClient = dohServersCN, p.httpClientCN
		fallback, fallbackClient = dohServersOut, p.httpClientOut
	} else {
		primary, primaryClient = dohServersOut, p.httpClientOut
		fallback, fallbackClient = dohServersCN, p.httpClientCN
	}

	var lastErr error
	for _, base := range primary {
		ips, err := p.dohQuery(primaryClient, base, domain)
		if err != nil {
			lastErr = err
			continue
		}
		if len(ips) > 0 {
			log.Printf("[DNS] %s → %v (via %s)", domain, ips, base)
			return ips, nil
		}
	}
	for _, base := range fallback {
		ips, err := p.dohQuery(fallbackClient, base, domain)
		if err != nil {
			lastErr = err
			continue
		}
		if len(ips) > 0 {
			log.Printf("[DNS] %s → %v (fallback via %s)", domain, ips, base)
			return ips, nil
		}
	}
	return nil, fmt.Errorf("all DoH failed: %v", lastErr)
}

func (p *DNSProxy) HandleDNSQuery(query []byte) ([]byte, error) {
	if len(query) < 12 {
		return nil, fmt.Errorf("DNS 查询太短")
	}
	domain := parseDNSQuestion(query)
	if domain == "" {
		return nil, fmt.Errorf("无法解析域名")
	}
	log.Printf("[DNS] 查询: %s", domain)

	ips, err := p.ResolveViaDoH(domain)
	if err != nil {
		log.Printf("[DNS] DoH 失败 %s: %v", domain, err)
		return buildDNSError(query, 2), nil
	}
	for _, ip := range ips {
		p.cache.Store(ip, domain, 5*time.Minute)
	}
	return buildDNSResponse(query, ips), nil
}

func parseDNSQuestion(query []byte) string {
	if len(query) < 12 {
		return ""
	}
	offset := 12
	var labels []string
	for offset < len(query) {
		l := int(query[offset])
		if l == 0 {
			break
		}
		if l > 63 {
			return ""
		}
		offset++
		if offset+l > len(query) {
			return ""
		}
		labels = append(labels, string(query[offset:offset+l]))
		offset += l
	}
	return strings.Join(labels, ".")
}

func buildDNSResponse(query []byte, ips []string) []byte {
	if len(query) < 12 {
		return nil
	}
	qnameEnd := 12
	for qnameEnd < len(query) {
		l := int(query[qnameEnd])
		if l == 0 {
			qnameEnd++
			break
		}
		qnameEnd += 1 + l
	}
	questionEnd := qnameEnd + 4
	if questionEnd > len(query) {
		return nil
	}
	resp := make([]byte, 0, 512)
	resp = append(resp, query[0], query[1], 0x81, 0x80, 0x00, 0x01, 0x00, byte(len(ips)), 0x00, 0x00, 0x00, 0x00)
	resp = append(resp, query[12:questionEnd]...)
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr).To4()
		if ip == nil {
			continue
		}
		resp = append(resp, 0xC0, 0x0C, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2C, 0x00, 0x04)
		resp = append(resp, ip...)
	}
	return resp
}

func buildDNSError(query []byte, rcode byte) []byte {
	if len(query) < 12 {
		return nil
	}
	qnameEnd := 12
	for qnameEnd < len(query) {
		l := int(query[qnameEnd])
		if l == 0 {
			qnameEnd++
			break
		}
		qnameEnd += 1 + l
	}
	questionEnd := qnameEnd + 4
	if questionEnd > len(query) {
		return nil
	}
	resp := make([]byte, 0, 512)
	resp = append(resp, query[0], query[1], 0x81, 0x80|rcode, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
	resp = append(resp, query[12:questionEnd]...)
	return resp
}
