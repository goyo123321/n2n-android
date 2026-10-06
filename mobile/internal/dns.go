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
	cache         *DNSCache
	httpClientCN  *http.Client // 国内 DoH（物理网络）
	httpClientOut *http.Client // 国外 DoH（走 Worker 出口）
}

// 国内 DoH
var dohServersCN = []string{
	"https://dns.alidns.com/dns-query",
	"https://doh.pub/dns-query",
	"https://dns.360.cn/dns-query",
}

// 国外 DoH（用域名做 URL，DialContext 强制连 CF IP）
var dohServersOut = []string{
	"https://cloudflare-dns.com/dns-query",
}

// Cloudflare DNS 的 Anycast IP
var dohCloudflareIPs = []string{
	"1.1.1.1",
	"1.0.0.1",
}

// NewDNSProxy 创建双 DNS 代理
//
//   - 国内域名 → 国内 DoH（走物理网络）
//   - 国外域名 → 远程 DoH（走 Worker 出口代理）
//
// getWSOutbound 延迟获取 wsOutbound（它是异步初始化的）
func NewDNSProxy(cache *DNSCache, getWSOutbound func() *WSOutbound) (*DNSProxy, error) {
	// 国内 DoH：protectedDialer 走物理网络
	protectedDialer := newProtectedDialer()
	transportCN := &http.Transport{
		DialContext:       protectedDialer.DialContext,
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
	}

	// 国外 DoH：走 Worker 出口代理
	transportOut := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			wsOut := getWSOutbound()
			if wsOut == nil {
				return nil, fmt.Errorf("wsOutbound 未就绪")
			}
			// addr 形如 "cloudflare-dns.com:443"，强制连 CF Anycast IP
			_, portStr, err := net.SplitHostPort(addr)
			if err != nil {
				portStr = "443"
			}
			port, _ := strconv.Atoi(portStr)

			// 依次尝试多个 CF IP
			for _, ip := range dohCloudflareIPs {
				stream, err := wsOut.NewStream(ip, port)
				if err == nil {
					return newStreamConn(stream, addr), nil
				}
			}
			return nil, fmt.Errorf("DoH 出站连接失败")
		},
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
	}

	return &DNSProxy{
		cache:         cache,
		httpClientCN:  &http.Client{Timeout: 5 * time.Second, Transport: transportCN},
		httpClientOut: &http.Client{Timeout: 8 * time.Second, Transport: transportOut},
	}, nil
}

// isChinaDomain 简化版国内域名判断
func isChinaDomain(domain string) bool {
	d := strings.ToLower(domain)
	if strings.HasSuffix(d, ".cn") {
		return true
	}
	cnSuffixes := []string{
		".baidu.com", ".qq.com", ".taobao.com", ".tmall.com", ".jd.com",
		".alipay.com", ".bilibili.com", ".weibo.com", ".163.com",
		".zhihu.com", ".csdn.net", ".aliyun.com", ".tencent.com",
		".cnblogs.com", ".sohu.com", ".sina.com.cn", ".toutiao.com",
		".douyin.com", ".kuaishou.com", ".meituan.com", ".dianping.com",
		".iqiyi.com", ".youku.com", ".mi.com", ".huawei.com",
		".netease.com", ".126.com", ".188.com", ".xunlei.com",
		".zol.com.cn", ".it168.com", ".pconline.com.cn", ".ithome.com",
	}
	for _, suf := range cnSuffixes {
		if strings.HasSuffix(d, suf) {
			return true
		}
	}
	return false
}

// dohQuery 向单个 DoH 服务器查询
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

func (p *DNSProxy) ResolveViaDoH(domain string) ([]string, error) {
	isCN := isChinaDomain(domain)

	var primaryServers, fallbackServers []string
	var primaryClient, fallbackClient *http.Client

	if isCN {
		primaryServers = dohServersCN
		primaryClient = p.httpClientCN
		fallbackServers = dohServersOut
		fallbackClient = p.httpClientOut
	} else {
		primaryServers = dohServersOut
		primaryClient = p.httpClientOut
		fallbackServers = dohServersCN
		fallbackClient = p.httpClientCN
	}

	var lastErr error

	// 主服务器组
	for _, base := range primaryServers {
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

	// Fallback 组
	for _, base := range fallbackServers {
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
