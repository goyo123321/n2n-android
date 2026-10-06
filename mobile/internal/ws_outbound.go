package internal

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WSOutbound struct {
	mu      sync.RWMutex
	stopped bool

	streamURL     string
	preferredIP   string
	preferredPort int
	sniHost       string

	// ★ TURN 客户端（Worker 失败时回退）
	turnClient *TURNClient
}

func NewWSOutbound(
	signalingURL, roomId, clientId, connectToken,
	preferredIP string, preferredPort int,
	turnClient *TURNClient,
) (*WSOutbound, error) {
	u, err := url.Parse(signalingURL)
	if err != nil {
		return nil, fmt.Errorf("解析 URL 失败: %w", err)
	}
	sniHost := u.Hostname()

	scheme := u.Scheme
	if scheme == "http" {
		scheme = "ws"
	} else if scheme == "https" {
		scheme = "wss"
	}
	streamURL := fmt.Sprintf("%s://%s/ws/out/stream/", scheme, u.Host)

	log.Printf("[WSOut] 出口端点: %s", streamURL)

	return &WSOutbound{
		streamURL:     streamURL,
		preferredIP:   preferredIP,
		preferredPort: preferredPort,
		sniHost:       sniHost,
		turnClient:    turnClient,
	}, nil
}

// NewStream 为一个 TCP 连接建立出口通道
//
// 优先顺序：
//   1. Worker /ws/out/stream/
//   2. Worker 失败 → TURN RFC 6062 TCP allocation（如果可用）
func (o *WSOutbound) NewStream(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	o.mu.RLock()
	if o.stopped {
		o.mu.RUnlock()
		return nil, fmt.Errorf("已关闭")
	}
	o.mu.RUnlock()

	// 1. Worker
	stream, err := o.tryWorker(targetIP, targetPort)
	if err == nil {
		return stream, nil
	}
	log.Printf("[WSOut] Worker 失败: %v", err)

	// 2. TURN RFC 6062 回退
	if o.turnClient != nil && o.turnClient.HasTCPAlloc() {
		log.Printf("[WSOut] 回退到 TURN RFC 6062")
		turnStream, turnErr := o.turnClient.DialTCP(targetIP, targetPort)
		if turnErr == nil {
			log.Printf("[WSOut] ✅ TURN 隧道建立 %s:%d", targetIP, targetPort)
			return turnStream, nil
		}
		log.Printf("[WSOut] TURN 回退也失败: %v", turnErr)
		return nil, fmt.Errorf("Worker 和 TURN 都失败: %v / %v", err, turnErr)
	}

	return nil, fmt.Errorf("Worker 失败且无 TURN 回退: %w", err)
}

func (o *WSOutbound) tryWorker(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		TLSClientConfig:  &tls.Config{ServerName: o.sniHost},
	}

	protectedDialer := newProtectedDialer()

	if o.preferredIP != "" {
		preferredHost := o.preferredIP
		preferredPortNum := o.preferredPort

		if h, pStr, err := net.SplitHostPort(o.preferredIP); err == nil {
			preferredHost = h
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 && p <= 65535 {
				preferredPortNum = p
			}
		}
		if preferredPortNum <= 0 {
			preferredPortNum = 443
		}

		targetAddr := net.JoinHostPort(preferredHost, strconv.Itoa(preferredPortNum))
		dialer.NetDial = func(network, addr string) (net.Conn, error) {
			return protectedDialer.Dial(network, targetAddr)
		}
	} else {
		dialer.NetDial = protectedDialer.Dial
	}

	conn, _, err := dialer.Dial(o.streamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("连接 Worker 失败: %w", err)
	}

	stream := &WSStream{
		ws:         conn,
		readCh:     make(chan []byte, 64),
		closed:     make(chan struct{}),
		remoteIP:   targetIP,
		remotePort: targetPort,
	}

	go func() {
		defer close(stream.readCh)
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				select {
				case <-stream.closed:
					return
				default:
				}
				stream.Close()
				return
			}
			if msgType == websocket.BinaryMessage {
				cp := make([]byte, len(data))
				copy(cp, data)
				select {
				case stream.readCh <- cp:
				case <-stream.closed:
					return
				}
			}
		}
	}()

	ip4 := net.ParseIP(targetIP).To4()
	if ip4 == nil {
		conn.Close()
		return nil, fmt.Errorf("仅支持 IPv4: %s", targetIP)
	}

	handshake := make([]byte, 7)
	handshake[0] = 0x01
	copy(handshake[1:5], ip4)
	binary.BigEndian.PutUint16(handshake[5:7], uint16(targetPort))

	if err := conn.WriteMessage(websocket.BinaryMessage, handshake); err != nil {
		conn.Close()
		return nil, fmt.Errorf("发送握手失败: %w", err)
	}

	log.Printf("[WSOut] Worker 隧道建立 %s:%d", targetIP, targetPort)
	return stream, nil
}

func (o *WSOutbound) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stopped = true
}

type WSStream struct {
	ws         *websocket.Conn
	readCh     chan []byte
	closed     chan struct{}
	closeOnce  sync.Once
	remoteIP   string
	remotePort int
}

func (s *WSStream) Read(p []byte) (int, error) {
	select {
	case data, ok := <-s.readCh:
		if !ok {
			return 0, io.EOF
		}
		return copy(p, data), nil
	case <-s.closed:
		return 0, io.EOF
	}
}

func (s *WSStream) Write(p []byte) (int, error) {
	select {
	case <-s.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	if err := s.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *WSStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.ws != nil {
			s.ws.Close()
		}
	})
	return nil
}
