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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TURNServerInfo struct {
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	TTL      uint32 `json:"ttl"`
}

type TURNResponse struct {
	Success bool             `json:"success"`
	Source  string           `json:"source,omitempty"`
	Servers []TURNServerInfo `json:"servers"`
	Error   string           `json:"error,omitempty"`
}

type TURNClient struct {
	mu           sync.RWMutex
	lite         *TURNLite
	tcpAlloc     *TURNTCPAllocation
	relayAddr    net.Addr
	server       *TURNServerInfo
	signalingURL string
	uuid         string
	edge         *Edge
	onMessage    func([]byte, net.Addr)
	stopCh       chan struct{}

	httpClient *http.Client
}

// NewTURNClient 创建 TURN 客户端。
//
// 关键：拉凭证时也走「优选 IP + SNI」，避免依赖 DNS 解析
// （系统 DNS 指向 TUN 虚拟 IP 时，解析域名会死循环）。
func NewTURNClient(signalingURL string, uuid string, edge *Edge) *TURNClient {
	protectedDialer := newProtectedDialer()

	// 从 signalingURL 提取 SNI host
	var sniHost string
	if u, err := url.Parse(signalingURL); err == nil {
		sniHost = u.Hostname()
	}

	// 从 edge.cfg 读取优选 IP
	var preferredAddr string
	if edge != nil && edge.cfg != nil && edge.cfg.PreferredIP != "" {
		host := edge.cfg.PreferredIP
		port := edge.cfg.PreferredPort
		if h, pStr, err := net.SplitHostPort(edge.cfg.PreferredIP); err == nil {
			host = h
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 && p <= 65535 {
				port = p
			}
		}
		if port <= 0 {
			port = 443
		}
		preferredAddr = net.JoinHostPort(host, strconv.Itoa(port))
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// ★ 有优选 IP 就直连 IP，跳过 DNS 解析
			if preferredAddr != "" {
				return protectedDialer.DialContext(ctx, network, preferredAddr)
			}
			return protectedDialer.DialContext(ctx, network, addr)
		},
		// ★ 用域名做 SNI 让证书验证通过，但 TCP 层连的是 IP
		TLSClientConfig:   &tls.Config{ServerName: sniHost},
		ForceAttemptHTTP2: false,
	}

	return &TURNClient{
		signalingURL: signalingURL,
		uuid:         uuid,
		edge:         edge,
		stopCh:       make(chan struct{}),
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}
}

func (tc *TURNClient) FetchAndSetup(ctx context.Context) error {
	httpBase := tc.signalingURL
	if strings.HasPrefix(httpBase, "wss://") {
		httpBase = "https://" + httpBase[len("wss://"):]
	} else if strings.HasPrefix(httpBase, "ws://") {
		httpBase = "http://" + httpBase[len("ws://"):]
	}
	httpBase = strings.TrimRight(httpBase, "/")

	credURL := fmt.Sprintf("%s/api/turn-credentials?ttl=86400", httpBase)
	if tc.uuid != "" {
		credURL += "&token=" + url.QueryEscape(tc.uuid)
	}

	log.Printf("[TURN] 请求凭证: %s", redactToken(credURL))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, credURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := tc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch credentials: %w", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("turn-credentials 未授权 (http=%d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("credentials endpoint returned %d", resp.StatusCode)
	}

	var creds TURNResponse
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return fmt.Errorf("decode credentials: %w", err)
	}
	if !creds.Success || len(creds.Servers) == 0 {
		return fmt.Errorf("no TURN servers: %s", creds.Error)
	}

	tc.mu.Lock()
	tc.server = &creds.Servers[0]
	tc.mu.Unlock()

	log.Printf("[TURN] 获取到 %s TURN: %s", creds.Source, creds.Servers[0].URL)
	return tc.setupAllocation(ctx)
}

func (tc *TURNClient) setupAllocation(ctx context.Context) error {
	tc.mu.RLock()
	srv := tc.server
	tc.mu.RUnlock()
	if srv == nil {
		return fmt.Errorf("TURN server not set")
	}

	turnAddr := srv.URL
	if i := strings.Index(turnAddr, "?"); i >= 0 {
		turnAddr = turnAddr[:i]
	}
	turnAddr = strings.TrimPrefix(turnAddr, "turn://")
	turnAddr = strings.TrimPrefix(turnAddr, "turns://")
	turnAddr = strings.TrimPrefix(turnAddr, "turn:")
	turnAddr = strings.TrimPrefix(turnAddr, "turns:")
	turnAddr = strings.TrimPrefix(turnAddr, "//")

	log.Printf("[TURN] 尝试 UDP transport: %s", turnAddr)
	lite := NewTURNLiteWithTCP(turnAddr, srv.Username, srv.Password, false)
	lite.onMessage = func(data []byte, addr net.Addr) {
		if tc.onMessage != nil {
			tc.onMessage(data, addr)
		}
	}
	if err := lite.Allocate(); err != nil {
		log.Printf("[TURN] UDP transport 失败: %v，尝试 TCP transport", err)
		lite2 := NewTURNLiteWithTCP(turnAddr, srv.Username, srv.Password, true)
		lite2.onMessage = func(data []byte, addr net.Addr) {
			if tc.onMessage != nil {
				tc.onMessage(data, addr)
			}
		}
		if err2 := lite2.Allocate(); err2 != nil {
			log.Printf("[TURN] UDP+TCP allocation 都失败: %v", err2)
			lite = nil
		} else {
			lite = lite2
		}
	}

	if lite != nil && lite.relayAddr != nil {
		tc.mu.Lock()
		tc.lite = lite
		tc.relayAddr = lite.relayAddr
		tc.mu.Unlock()
		log.Printf("[TURN] ✅ UDP 就绪: %s", tc.relayAddr)
	}

	log.Printf("[TURN] 尝试 RFC 6062 TCP allocation: %s", turnAddr)
	tcpAlloc := NewTURNTCPAllocation(turnAddr, srv.Username, srv.Password)
	if err := tcpAlloc.Allocate(); err != nil {
		log.Printf("[TURN] RFC 6062 不可用: %v（不影响 P2P 中继）", err)
	} else {
		tc.mu.Lock()
		tc.tcpAlloc = tcpAlloc
		tc.mu.Unlock()
		log.Printf("[TURN] ✅ RFC 6062 TCP allocation 就绪: %s", tcpAlloc.GetRelayAddr())
	}

	if tc.lite == nil && tc.tcpAlloc == nil {
		return fmt.Errorf("TURN 完全不可用")
	}
	return nil
}

func (tc *TURNClient) Send(data []byte, remoteAddr net.Addr) error {
	tc.mu.RLock()
	lite := tc.lite
	tc.mu.RUnlock()
	if lite == nil {
		return fmt.Errorf("TURN UDP 未就绪")
	}
	udp, ok := remoteAddr.(*net.UDPAddr)
	if !ok {
		return fmt.Errorf("TURN 只支持 UDP 地址")
	}
	return lite.SendTo(data, udp)
}

func (tc *TURNClient) DialTCP(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	tc.mu.RLock()
	tcpAlloc := tc.tcpAlloc
	tc.mu.RUnlock()
	if tcpAlloc == nil {
		return nil, fmt.Errorf("TURN RFC 6062 未就绪")
	}
	return tcpAlloc.Dial(targetIP, targetPort)
}

func (tc *TURNClient) HasTCPAlloc() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.tcpAlloc != nil
}

func (tc *TURNClient) GetRelayAddr() string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	if tc.relayAddr == nil {
		return ""
	}
	return tc.relayAddr.String()
}

func (tc *TURNClient) IsReady() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.lite != nil
}

func (tc *TURNClient) Close() {
	select {
	case <-tc.stopCh:
	default:
		close(tc.stopCh)
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.lite != nil {
		tc.lite.Close()
		tc.lite = nil
	}
	if tc.tcpAlloc != nil {
		tc.tcpAlloc.Close()
		tc.tcpAlloc = nil
	}
}

func redactToken(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	if q.Get("token") != "" {
		q.Set("token", "***")
		u.RawQuery = q.Encode()
	}
	return u.String()
}
