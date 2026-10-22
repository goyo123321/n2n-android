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

// ★ 新增：读超时。
//
// 客户端每 20 秒发一次 ping，服务端每 30 秒主动发 server_ping。
// 90 秒内没收到任何消息 → 认为下行通道已死（TCP 表面正常但服务端
// 消息到达不了客户端）→ 主动重连。
//
// 旧版缺这个超时，遇到"下行僵死"会永远阻塞在 ReadMessage()，
// 只有等到下次 ws.Send 写失败才触发重连——重连延迟可能达到
// 数分钟。加读超时后最多 90 秒就恢复。
const wsReadTimeout = 90 * time.Second

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

	// ★ P1 修复：heartbeat 只在这里启动一次。
	//   tryReconnect 不再启动 heartbeat（否则重连 N 次后泄漏 N 个 goroutine）。
	//   heartbeat 内部通过 ws.conn 读取当前连接，断线时 Send 返回 nil/error
	//   都继续循环（不再 return 退出），重连后自动恢复心跳。
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

// readLoop 读循环。
//
// ★ 改动（本轮）：
//   1. 每次 ReadMessage 前设置 wsReadTimeout 读超时
//   2. 识别 net.Error.Timeout() → 视为"下行僵死"
//   3. 收到 server_ping → 立即回 server_pong，不路由给上层
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
		// ★ 新增：设置读超时
		_ = conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

		msgType, data, err := conn.ReadMessage()
		if err != nil {
			ws.reconnectMu.Lock()
			stopping := ws.stopping
			ws.reconnectMu.Unlock()

			if stopping {
				return
			}

			// ★ 新增：区分读超时与其他错误
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				log.Printf("[WS] 读超时 %v 未收到任何消息（下行僵死），触发重连", wsReadTimeout)
			} else {
				log.Printf("[WS] 读取错误: %v", err)
			}
			safeGo("ws-reconnect", ws.tryReconnect)
			return
		}

		if msgType == websocket.TextMessage {
			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			// ★ 新增：服务端主动 ping → 立即回 pong，不路由
			if t, _ := msg["type"].(string); t == "server_ping" {
				_ = ws.Send(map[string]interface{}{"type": "server_pong"})
				continue
			}

			ws.routeText(msg)
		} else if msgType == websocket.BinaryMessage {
			ws.routeBinary(data)
		}
	}
}

// tryReconnect 无限重试，指数退避封顶 5 分钟。
//
// ★ P1 修复（本轮）：重连成功后只重启 readLoop，
//   **不再启动 heartbeat**——heartbeat 在 NewWSTransport 中已启动一次，
//   通过 ws.conn 读最新连接，不随重连重启。修复旧版重连 N 次后
//   N 个 heartbeat goroutine 并存的泄漏问题。
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

	backoff := 1 * time.Second
	const maxBackoff = 5 * time.Minute
	attempt := 0

	for {
		select {
		case <-ws.stopCh:
			log.Printf("[WS] 收到停止信号，退出重连")
			return
		case <-time.After(backoff):
		}

		attempt++
		log.Printf("[WS] 重连 (第 %d 次, 等待 %v)...", attempt, backoff)

		conn, _, err := ws.dialer.Dial(ws.fullURL, nil)
		if err != nil {
			log.Printf("[WS] 重连失败: %v", err)

			// 指数退避：1s → 2s → 5s → 10s → 30s → 60s → 2m → 5m (封顶)
			switch {
			case backoff < 2*time.Second:
				backoff = 2 * time.Second
			case backoff < 5*time.Second:
				backoff = 5 * time.Second
			case backoff < 10*time.Second:
				backoff = 10 * time.Second
			case backoff < 30*time.Second:
				backoff = 30 * time.Second
			case backoff < 60*time.Second:
				backoff = 60 * time.Second
			case backoff < 2*time.Minute:
				backoff = 2 * time.Minute
			default:
				backoff = maxBackoff
			}
			continue
		}

		ws.mu.Lock()
		ws.conn = conn
		ws.mu.Unlock()
		log.Printf("[WS] ✅ 重连成功（第 %d 次尝试）", attempt)

		// ★ P1 修复：只重启 readLoop，不再启动 heartbeat
		safeGo("ws-readLoop", func() { ws.readLoop(conn) })
		ws.routeText(map[string]interface{}{"type": "_reconnected"})
		return
	}
}

// heartbeat 每 20 秒发一次 ping。
//
// ★ P1 修复（本轮）：Send 失败（连接断开）时 **continue** 而非 return。
//   旧版只要一次 Send 失败就永久停掉心跳，直到进程重启。改为 continue 后
//   断线期间静默跳过（Send 内部 conn==nil 时返回 nil），重连后自动恢复。
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
			// ★ P1 修复：continue 而非 return
			// 断线期间 Send 会返回 nil（conn==nil 时静默），真正的错误（如
			// 写超时）也继续循环，重连后自动恢复。
			continue
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
		return nil
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
