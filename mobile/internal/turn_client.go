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

// TURNClient TURN 主控。
//
// ★ 只做 UDP transport（RFC 5766）+ TCP transport 兜底，不做 RFC 6062。
//   RFC 6062（TURN TCP allocation + ConnectionBind 隧道）在本项目里
//   从未被数据面使用——旧代码分配了 allocation 却不发一字节，
//   白占 TURN 服务器并发槽位。已删除。
//
//   如果将来真要加 TCP 中继路径，需要：
//   1. 客户端两侧都做 RFC 6062 allocation
//   2. 服务端协调下发 turnTCPConnectRequest 指令
//   3. 一侧 DialTCP 得到 io.ReadWriteCloser
//   4. 套 4 字节长度前缀复用现有 framer
//   5. relay_fallback.go 加 ConnTURN-TCP 状态
//   6. 服务端透传对端 relay addr
//   这是一个独立功能（200+ 行 + 服务端协议扩展），不在当前范围。
type TURNClient struct {
	mu           sync.RWMutex
	lite         *TURNLite
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

// setupAllocation 尝试建立 TURN allocation。
//
// ★ 只用 UDP transport（RFC 5766）。失败时尝试 TCP transport（同样的
//   RFC 5766 协议但走 TCP），两者都不依赖 RFC 6062。
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

	// ★ UDP transport（主路径）
	log.Printf("[TURN] 尝试 UDP transport: %s", turnAddr)
	lite := NewTURNLiteWithTCP(turnAddr, srv.Username, srv.Password, false)
	lite.onMessage = func(data []byte, addr net.Addr) {
		if tc.onMessage != nil {
			tc.onMessage(data, addr)
		}
	}
	if err := lite.Allocate(); err != nil {
		// ★ UDP 被运营商封时，回退到 TCP transport（同一个 TURN 服务器）
		log.Printf("[TURN] UDP transport 失败: %v，尝试 TCP transport", err)
		lite2 := NewTURNLiteWithTCP(turnAddr, srv.Username, srv.Password, true)
		lite2.onMessage = func(data []byte, addr net.Addr) {
			if tc.onMessage != nil {
				tc.onMessage(data, addr)
			}
		}
		if err2 := lite2.Allocate(); err2 != nil {
			log.Printf("[TURN] UDP+TCP transport 都失败: %v", err2)
			return fmt.Errorf("TURN 完全不可用: %v", err2)
		}
		lite = lite2
	}

	tc.mu.Lock()
	tc.lite = lite
	tc.relayAddr = lite.relayAddr
	tc.mu.Unlock()

	if lite.useTCP {
		log.Printf("[TURN] ✅ TCP transport 就绪: %s", tc.relayAddr)
	} else {
		log.Printf("[TURN] ✅ UDP transport 就绪: %s", tc.relayAddr)
	}
	return nil
}

func (tc *TURNClient) Send(data []byte, remoteAddr net.Addr) error {
	tc.mu.RLock()
	lite := tc.lite
	tc.mu.RUnlock()
	if lite == nil {
		return fmt.Errorf("TURN 未就绪")
	}
	udp, ok := remoteAddr.(*net.UDPAddr)
	if !ok {
		return fmt.Errorf("TURN 只支持 UDP 地址")
	}
	return lite.SendTo(data, udp)
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
