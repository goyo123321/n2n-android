package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
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
	relayAddr    net.Addr
	server       *TURNServerInfo
	signalingURL string
	connectToken string
	edge         *Edge
	onMessage    func([]byte, net.Addr)
	stopCh       chan struct{}
}

func NewTURNClient(signalingURL string, connectToken string, edge *Edge) *TURNClient {
	return &TURNClient{
		signalingURL: signalingURL,
		connectToken: connectToken,
		edge:         edge,
		stopCh:       make(chan struct{}),
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
	if tc.connectToken != "" {
		credURL += "&token=" + url.QueryEscape(tc.connectToken)
	}

	log.Printf("[TURN] 请求凭证: %s", redactToken(credURL))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, credURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch credentials: %w", err)
	}
	defer resp.Body.Close()

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

	// ★ 先剥离 ?transport=xxx（防御：即使服务器未剥离也能用）
	if i := strings.Index(turnAddr, "?"); i >= 0 {
		turnAddr = turnAddr[:i]
	}

	turnAddr = strings.TrimPrefix(turnAddr, "turn://")
	turnAddr = strings.TrimPrefix(turnAddr, "turns://")
	turnAddr = strings.TrimPrefix(turnAddr, "turn:")
	turnAddr = strings.TrimPrefix(turnAddr, "turns:")
	turnAddr = strings.TrimPrefix(turnAddr, "//")

	// UDP transport
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
			return fmt.Errorf("TURN 全部失败: UDP=%v TCP=%v", err, err2)
		}
		lite = lite2
	}

	tc.mu.Lock()
	tc.lite = lite
	tc.relayAddr = lite.relayAddr
	tc.mu.Unlock()

	log.Printf("[TURN] ✅ 就绪: %s", tc.relayAddr)
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

var _ = time.Now
