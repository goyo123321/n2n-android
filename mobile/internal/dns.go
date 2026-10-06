package internal

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type DNSCache struct {
	mu      sync.RWMutex
	ipToDom map[string]string
	domToIP map[string][]string
	ttl     map[string]time.Time
}

func NewDNSCache() *DNSCache {
	return &DNSCache{
		ipToDom: make(map[string]string),
		domToIP: make(map[string][]string),
		ttl:     make(map[string]time.Time),
	}
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
	cache      *DNSCache
	httpClient *http.Client
}

// NewDNSProxy 创建 DNS 代理
// ★ DoH 请求通过 protected dialer 走物理网络（不绕 TUN）
func NewDNSProxy(cache *DNSCache) (*DNSProxy, error) {
	// ★ protected dialer：DoH HTTPS socket 创建后调 VpnService.protect() 绕过 VPN
	protectedDialer := newProtectedDialer()

	transport := &http.Transport{
		DialContext:       protectedDialer.DialContext,
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
	}

	return &DNSProxy{
		cache:      cache,
		httpClient: &http.Client{Timeout: 5 * time.Second, Transport: transport},
	}, nil
}

func (p *DNSProxy) ResolveViaDoH(domain string) ([]string, error) {
	url := fmt.Sprintf("https://1.1.1.1/dns-query?name=%s&type=A", domain)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Answer []struct {
			Name string `json:"name"`
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
	resp = append(resp, query[0], query[1])
	resp = append(resp, 0x81, 0x80)
	resp = append(resp, 0x00, 0x01)
	resp = append(resp, 0x00, byte(len(ips)))
	resp = append(resp, 0x00, 0x00)
	resp = append(resp, 0x00, 0x00)
	resp = append(resp, query[12:questionEnd]...)

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr).To4()
		if ip == nil {
			continue
		}
		resp = append(resp, 0xC0, 0x0C)
		resp = append(resp, 0x00, 0x01)
		resp = append(resp, 0x00, 0x01)
		resp = append(resp, 0x00, 0x00, 0x01, 0x2C)
		resp = append(resp, 0x00, 0x04)
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
	resp = append(resp, query[0], query[1])
	resp = append(resp, 0x81, 0x80|rcode)
	resp = append(resp, 0x00, 0x01)
	resp = append(resp, 0x00, 0x00)
	resp = append(resp, 0x00, 0x00)
	resp = append(resp, 0x00, 0x00)
	resp = append(resp, query[12:questionEnd]...)
	return resp
}
