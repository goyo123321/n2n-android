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

	"github.com/pion/turn/v4"
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
	client       *turn.Client
	relayConn    net.PacketConn
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, credURL, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("credentials endpoint %d", resp.StatusCode)
	}

	var creds TURNResponse
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return err
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
	turnAddr = strings.TrimPrefix(turnAddr, "turn://")
	turnAddr = strings.TrimPrefix(turnAddr, "turns://")
	turnAddr = strings.TrimPrefix(turnAddr, "turn:")
	turnAddr = strings.TrimPrefix(turnAddr, "turns:")
	turnAddr = strings.TrimPrefix(turnAddr, "//")

	cfg := &turn.ClientConfig{
		TURNServerAddr: turnAddr,
		Username:       srv.Username,
		Password:       srv.Password,
	}
	if strings.Contains(strings.ToLower(srv.URL), "cloudflare") {
		cfg.Realm = "cloudflare"
	}

	turnClient, err := turn.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("create TURN client: %w", err)
	}
	if err := turnClient.Listen(); err != nil {
		turnClient.Close()
		return err
	}
	relayConn, err := turnClient.Allocate()
	if err != nil {
		turnClient.Close()
		return fmt.Errorf("allocate: %w", err)
	}

	tc.mu.Lock()
	tc.client = turnClient
	tc.relayConn = relayConn
	tc.relayAddr = relayConn.LocalAddr()
	tc.mu.Unlock()

	log.Printf("[TURN] 中继地址: %s", tc.relayAddr)
	go tc.readLoop()
	return nil
}

func (tc *TURNClient) readLoop() {
	buf := make([]byte, 65535)
	for {
		select {
		case <-tc.stopCh:
			return
		default:
		}
		tc.mu.RLock()
		conn := tc.relayConn
		tc.mu.RUnlock()
		if conn == nil {
			return
		}
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-tc.stopCh:
				return
			default:
			}
			return
		}
		if tc.onMessage != nil && n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			tc.onMessage(data, addr)
		}
	}
}

func (tc *TURNClient) Send(data []byte, remoteAddr net.Addr) error {
	tc.mu.RLock()
	conn := tc.relayConn
	tc.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("TURN 未就绪")
	}
	_, err := conn.WriteTo(data, remoteAddr)
	return err
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
	return tc.relayConn != nil
}

func (tc *TURNClient) Close() {
	select {
	case <-tc.stopCh:
	default:
		close(tc.stopCh)
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.client != nil {
		tc.client.Close()
		tc.client = nil
	}
	if tc.relayConn != nil {
		tc.relayConn.Close()
		tc.relayConn = nil
	}
}
