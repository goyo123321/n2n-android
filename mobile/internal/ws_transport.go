package internal

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
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
	dialer      *websocket.Dialer
	stopCh      chan struct{}
	reconnectMu sync.Mutex
	stopping    bool
}

// NewWSTransport 建立信令 WebSocket。
// uuid 用于 URL 认证（服务端按 ?token= 读取），同时可用于日志标识。
func NewWSTransport(signalingURL, roomId, clientId, uuid, preferredIP string, preferredPort int) (*WSTransport, error) {
	base := strings.TrimRight(signalingURL, "/")
	fullURL := base + "/ws/" + url.PathEscape(roomId) + "?cid=" + url.QueryEscape(clientId)
	if uuid != "" {
		// 服务端按 query 参数 "token" 读取（保持向后兼容）
		fullURL += "&token=" + url.QueryEscape(uuid)
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

	protectedDialer := newProtectedDialer()

	if preferredIP != "" {
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

		dialer.TLSClientConfig = &tls.Config{ServerName: host}
		dialer.NetDial = func(network, addr string) (net.Conn, error) {
			return protectedDialer.Dial(network, targetAddr)
		}
	} else {
		log.Printf("[WS] DNS 模式: %s", host)
		dialer.TLSClientConfig = &tls.Config{ServerName: host}
		dialer.NetDial = protectedDialer.Dial
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
		dialer:   dialer,
		stopCh:   make(chan struct{}),
	}
	go ws.readLoop(conn)
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

func (ws *WSTransport) readLoop(conn *websocket.Conn) {
	for {
		msgType, data, err := conn.ReadMessage()
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

		conn, _, err := ws.dialer.Dial(ws.fullURL, nil)
		if err != nil {
			log.Printf("[WS] 重连失败: %v", err)
			continue
		}
		ws.mu.Lock()
		ws.conn = conn
		ws.mu.Unlock()
		log.Printf("[WS] ✅ 重连成功")
		go ws.readLoop(conn)
		go ws.heartbeat(20 * time.Second)
		if ws.onMessage != nil {
			ws.onMessage(map[string]interface{}{"type": "_reconnected"})
		}
		return
	}

	log.Printf("[WS] 重连 10 次全部失败，放弃")
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
	data, _ := json.Marshal(msg)
	ws.mu.Lock()
	conn := ws.conn
	ws.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("WS 未连接")
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

func (ws *WSTransport) SendBinary(data []byte) error {
	ws.mu.Lock()
	conn := ws.conn
	ws.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("WS 未连接")
	}
	return conn.WriteMessage(websocket.BinaryMessage, data)
}

func (ws *WSTransport) Close() error {
	ws.reconnectMu.Lock()
	if ws.stopping {
		ws.reconnectMu.Unlock()
		return nil
	}
	ws.stopping = true
	close(ws.stopCh)
	ws.reconnectMu.Unlock()

	ws.mu.Lock()
	conn := ws.conn
	ws.conn = nil
	ws.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
