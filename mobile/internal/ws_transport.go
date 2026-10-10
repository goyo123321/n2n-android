package internal

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WSTransport struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	writeMu   sync.Mutex
	clientId  string
	onMessage func(map[string]interface{})
	onBinary  func([]byte)

	earlyText   []map[string]interface{}
	earlyBinary [][]byte
	handlersSet bool

	fullURL  string
	dialer   *websocket.Dialer
	stopCh   chan struct{}
	stopOnce sync.Once

	reconnectMu  sync.Mutex
	stopping     bool
	reconnecting bool
}

func (ws *WSTransport) SetHandlers(
	onMessage func(map[string]interface{}),
	onBinary func([]byte),
) {
	ws.mu.Lock()
	ws.onMessage = onMessage
	ws.onBinary = onBinary
	ws.handlersSet = true
	textBuf := ws.earlyText
	binBuf := ws.earlyBinary
	ws.earlyText = nil
	ws.earlyBinary = nil
	ws.mu.Unlock()

	if len(textBuf) > 0 {
		log.Printf("[WS] 重放 %d 条早期文本消息", len(textBuf))
		for _, msg := range textBuf {
			ws.routeText(msg)
		}
	}
	if len(binBuf) > 0 {
		log.Printf("[WS] 重放 %d 条早期二进制消息", len(binBuf))
		for _, data := range binBuf {
			ws.routeBinary(data)
		}
	}
}

func (ws *WSTransport) routeText(msg map[string]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WS] onMessage panic: %v\n%s", r, debug.Stack())
		}
	}()
	ws.mu.Lock()
	ready := ws.handlersSet && ws.onMessage != nil
	handler := ws.onMessage
	if !ready {
		ws.earlyText = append(ws.earlyText, msg)
	}
	ws.mu.Unlock()
	if ready {
		handler(msg)
	}
}

func (ws *WSTransport) routeBinary(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WS] onBinary panic: %v\n%s", r, debug.Stack())
		}
	}()
	ws.mu.Lock()
	ready := ws.handlersSet && ws.onBinary != nil
	handler := ws.onBinary
	if !ready {
		cp := make([]byte, len(data))
		copy(cp, data)
		ws.earlyBinary = append(ws.earlyBinary, cp)
	}
	ws.mu.Unlock()
	if ready {
		handler(data)
	}
}

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

	safeGo("ws-readLoop", func() { ws.readLoop(conn) })
	safeGo("ws-heartbeat", func() { ws.heartbeat(20 * time.Second) })
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
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WS] readLoop panic: %v\n%s", r, debug.Stack())
			ws.reconnectMu.Lock()
			stopping := ws.stopping
			ws.reconnectMu.Unlock()
			if !stopping {
				safeGo("ws-reconnect", ws.tryReconnect)
			}
		}
	}()

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
			safeGo("ws-reconnect", ws.tryReconnect)
			return
		}
		if msgType == websocket.TextMessage {
			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			ws.routeText(msg)
		} else if msgType == websocket.BinaryMessage {
			ws.routeBinary(data)
		}
	}
}

func (ws *WSTransport) tryReconnect() {
	ws.reconnectMu.Lock()
	if ws.stopping || ws.reconnecting {
		ws.reconnectMu.Unlock()
		return
	}
	ws.reconnecting = true
	ws.reconnectMu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WS] tryReconnect panic: %v\n%s", r, debug.Stack())
		}
		ws.reconnectMu.Lock()
		ws.reconnecting = false
		ws.reconnectMu.Unlock()
	}()

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
		safeGo("ws-readLoop", func() { ws.readLoop(conn) })
		safeGo("ws-heartbeat", func() { ws.heartbeat(20 * time.Second) })
		ws.routeText(map[string]interface{}{"type": "_reconnected"})
		return
	}

	log.Printf("[WS] 重连 10 次全部失败，放弃")
}

func (ws *WSTransport) heartbeat(interval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WS] heartbeat panic: %v\n%s", r, debug.Stack())
		}
	}()
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
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()

	ws.mu.Lock()
	conn := ws.conn
	ws.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("WS 未连接")
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

func (ws *WSTransport) SendBinary(data []byte) error {
	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()

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
	already := ws.stopping
	ws.stopping = true
	ws.reconnectMu.Unlock()

	ws.stopOnce.Do(func() { close(ws.stopCh) })

	if already {
		return nil
	}

	ws.mu.Lock()
	conn := ws.conn
	ws.conn = nil
	ws.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
