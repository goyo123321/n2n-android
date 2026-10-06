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
	mu        sync.RWMutex
	stopped   bool
	onMessage func([]byte, *net.UDPAddr)

	streamURL     string
	preferredIP   string
	preferredPort int
	sniHost       string
}

func NewWSOutbound(
	signalingURL, roomId, clientId, connectToken,
	preferredIP string, preferredPort int,
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
	}, nil
}

func (o *WSOutbound) NewStream(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	o.mu.RLock()
	if o.stopped {
		o.mu.RUnlock()
		return nil, fmt.Errorf("已关闭")
	}
	o.mu.RUnlock()

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		TLSClientConfig:  &tls.Config{ServerName: o.sniHost},
	}

	// ★ protected dialer
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

	log.Printf("[WSOut] 连接 %s (SNI=%s)", o.streamURL, o.sniHost)

	conn, _, err := dialer.Dial(o.streamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("连接出口失败: %w", err)
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
				log.Printf("[WSStream] 读错误: %v", err)
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
			} else if msgType == websocket.TextMessage {
				log.Printf("[WSStream] 服务端文本: %s", string(data))
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

	log.Printf("[WSStream] 建立 %s:%d", targetIP, targetPort)
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
		log.Printf("[WSStream] 关闭 %s:%d", s.remoteIP, s.remotePort)
	})
	return nil
}
