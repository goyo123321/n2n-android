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
	writeMu   sync.Mutex
	clientId  string
	onMessage func(map[string]interface{})
	onBinary  func([]byte)

	// ★ 早期消息缓冲：onMessage / onBinary 未设置前收到的消息暂存这里
	//   修复 "ready 消息在 handler 设置前到达被丢弃" 的问题
	earlyText   []map[string]interface{}
	earlyBinary [][]byte
	handlersSet bool

	fullURL     string
	dialer      *websocket.Dialer
	stopCh      chan struct{}
	reconnectMu sync.Mutex
	stopping    bool
}

// SetHandlers 设置 onMessage / onBinary 回调，并重放缓冲消息。
//
// ★ 替换原来直接赋值 ws.onMessage = xxx 的写法。
//   必须在 WS 建立后尽快调用，重放前收到的消息都在缓冲区里。
//   这是修复"ready 丢失导致 VIP / serverSeenIP 拿不到"的关键。
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
			if onMessage != nil {
				onMessage(msg)
			}
		}
	}
	if len(binBuf) > 0 {
		log.Printf("[WS] 重放 %d 条早期二进制消息", len(binBuf))
		for _, data := range binBuf {
			if onBinary != nil {
				onBinary(data)
			}
		}
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
			ws.mu.Lock()
			if ws.handlersSet && ws.onMessage != nil {
				handler := ws.onMessage
				ws.mu.Unlock()
				handler(msg)
			} else {
				// ★ 缓冲早期消息（handler 还没设置）
				ws.earlyText = append(ws.earlyText, msg)
				ws.mu.Unlock()
			}
		} else if msgType == websocket.BinaryMessage {
			ws.mu.Lock()
			if ws.handlersSet && ws.onBinary != nil {
				handler := ws.onBinary
				ws.mu.Unlock()
				handler(data)
			} else {
				// ★ 缓冲早期二进制消息
				cp := make([]byte, len(data))
				copy(cp, data)
				ws.earlyBinary = append(ws.earlyBinary, cp)
				ws.mu.Unlock()
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
		handler := ws.onMessage
		ws.mu.Unlock()
		log.Printf("[WS] ✅ 重连成功")
		go ws.readLoop(conn)
		go ws.heartbeat(20 * time.Second)
		if handler != nil {
			handler(map[string]interface{}{"type": "_reconnected"})
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
