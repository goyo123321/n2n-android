package internal

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WSTransport struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	clientId  string
	onMessage func(map[string]interface{})
	onBinary  func([]byte)

	fullURL     string
	stopCh      chan struct{}
	reconnectMu sync.Mutex
	stopping    bool
}

// NewWSTransport 建立 WebSocket 连接
//
// preferredIP 支持两种格式：
//   "104.17.217.162"         → 默认端口 443（或 scheme 对应端口）
//   "104.17.217.162:8443"    → 指定端口
//
// 优选 IP 时：
//   - TCP 连接目标：优选 IP:port
//   - URL：保持原域名（wss://原域名/ws/xxx）
//   - SNI：原域名
//   - HTTP Host header：原域名
func NewWSTransport(signalingURL, roomId, clientId, connectToken, preferredIP string, preferredPort int) (*WSTransport, error) {
	base := strings.TrimRight(signalingURL, "/")
	fullURL := base + "/ws/" + url.PathEscape(roomId) + "?cid=" + url.QueryEscape(clientId)
	if connectToken != "" {
		fullURL += "&token=" + url.QueryEscape(connectToken)
	}

	u, err := url.Parse(fullURL)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()

	defaultPort := 443
	if u.Scheme == "ws" {
		defaultPort = 80
	}

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// ★ 判断是否用优选 IP
	if preferredIP != "" {
		// 解析 ip:port
		preferredHost := preferredIP
		preferredPortNum := preferredPort

		if h, pStr, err := net.SplitHostPort(preferredIP); err == nil {
			preferredHost = h
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 && p <= 65535 {
				preferredPortNum = p
			}
		}
		if preferredPortNum <= 0 {
			preferredPortNum = defaultPort
		}

		targetAddr := net.JoinHostPort(preferredHost, strconv.Itoa(preferredPortNum))
		log.Printf("[WS] 优选 IP: %s (SNI=%s)", targetAddr, host)

		// ★ SNI 用原域名
		dialer.TLSClientConfig = &tls.Config{
			ServerName: host,
		}

		// ★ 关键：自定义 TCP 拨号，URL 保持原域名
		dialer.NetDial = func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: 10 * time.Second}
			// ★ 忽略 addr（原本是 url.jyece.kdns.fr:443），改连优选 IP
			return d.Dial(network, targetAddr)
		}
	} else {
		log.Printf("[WS] DNS 模式: %s", host)
		dialer.TLSClientConfig = &tls.Config{
			ServerName: host,
		}
	}

	log.Printf("[WS] 连接 %s", maskToken(fullURL))

	conn, _, err := dialer.Dial(fullURL, nil)
	if err != nil {
		return nil, err
	}
	ws := &WSTransport{
		conn:     conn,
		clientId: clientId,
		fullURL:  fullURL,
		stopCh:   make(chan struct{}),
	}
	go ws.readLoop()
	go ws.heartbeat(20 * time.Second)
	return ws, nil
}

func maskToken(u string) string {
	if !strings.Contains(u, "token=") {
		return u
	}
	parts := strings.Split(u, "token=")
	if len(parts) != 2 {
		return u
	}
	return parts[0] + "token=***"
}

func (ws *WSTransport) readLoop() {
	for {
		msgType, data, err := ws.conn.ReadMessage()
		if err != nil {
			log.Printf("[WS] 读取错误: %v", err)

			ws.reconnectMu.Lock()
			stopping := ws.stopping
			ws.reconnectMu.Unlock()

			if stopping {
				return
			}
			go ws.tryReconnect()
			return
		}
		if msgType == websocket.TextMessage {
			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if ws.onMessage != nil {
				ws.onMessage(msg)
			}
		} else if msgType == websocket.BinaryMessage {
			if ws.onBinary != nil {
				ws.onBinary(data)
			}
		}
	}
}

func (ws *WSTransport) tryReconnect() {
	ws.reconnectMu.Lock()
	defer ws.reconnectMu.Unlock()
	if ws.stopping {
		return
	}

	delays := []time.Duration{1, 2, 5, 10, 30, 60, 60, 60, 60, 60}
	for i, d := range delays {
		select {
		case <-ws.stopCh:
			return
		case <-time.After(d * time.Second):
		}
		log.Printf("[WS] 重连 (%d/10)...", i+1)
		conn, _, err := websocket.DefaultDialer.Dial(ws.fullURL, nil)
		if err != nil {
			continue
		}
		ws.mu.Lock()
		ws.conn = conn
		ws.mu.Unlock()
		log.Printf("[WS] ✅ 重连成功")
		go ws.readLoop()
		go ws.heartbeat(20 * time.Second)
		if ws.onMessage != nil {
			ws.onMessage(map[string]interface{}{"type": "_reconnected"})
		}
		return
	}
}

func (ws *WSTransport) heartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-ws.stopCh:
			return
		default:
		}
		if err := ws.Send(map[string]interface{}{
			"type": "ping",
			"ts":   time.Now().Unix(),
		}); err != nil {
			return
		}
	}
}

func (ws *WSTransport) Send(msg map[string]interface{}) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	data, _ := json.Marshal(msg)
	return ws.conn.WriteMessage(websocket.TextMessage, data)
}

func (ws *WSTransport) SendBinary(data []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (ws *WSTransport) Close() error {
	ws.reconnectMu.Lock()
	ws.stopping = true
	close(ws.stopCh)
	ws.reconnectMu.Unlock()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.conn.Close()
}
