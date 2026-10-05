package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type WSOutbound struct {
	ws            *WSTransport
	mu            sync.RWMutex
	stopped       bool
	onMessage     func([]byte, *net.UDPAddr)

	streamURL     string
	roomId        string
	clientId      string
	connectToken  string
	preferredIP   string
	preferredPort int
}

func NewWSOutbound(signalingURL, roomId, clientId, connectToken, preferredIP string, preferredPort int) (*WSOutbound, error) {
	mainURL := signalingURL
	if idx := strings.Index(mainURL, "/ws/"); idx >= 0 {
		mainURL = mainURL[:idx] + "/ws/out/"
	} else {
		mainURL = strings.TrimRight(mainURL, "/") + "/ws/out/"
	}

	streamURL := signalingURL
	if idx := strings.Index(streamURL, "/ws/"); idx >= 0 {
		streamURL = streamURL[:idx] + "/ws/out/stream/"
	} else {
		streamURL = strings.TrimRight(streamURL, "/") + "/ws/out/stream/"
	}

	ws, err := NewWSTransport(mainURL, roomId, clientId, connectToken, preferredIP, preferredPort)
	if err != nil {
		return nil, err
	}

	o := &WSOutbound{
		ws:            ws,
		streamURL:     streamURL,
		roomId:        roomId,
		clientId:      clientId,
		connectToken:  connectToken,
		preferredIP:   preferredIP,
		preferredPort: preferredPort,
	}
	ws.onBinary = o.handleBinary
	ws.onMessage = func(msg map[string]interface{}) {}
	return o, nil
}

func (o *WSOutbound) handleBinary(data []byte) {
	if len(data) < 6 {
		return
	}
	ip := net.IPv4(data[0], data[1], data[2], data[3])
	port := int(binary.BigEndian.Uint16(data[4:6]))
	payload := data[6:]
	if o.onMessage != nil {
		o.onMessage(payload, &net.UDPAddr{IP: ip, Port: port})
	}
}

func (o *WSOutbound) NewStream(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	ws, err := NewWSTransport(o.streamURL, o.roomId, o.clientId, o.connectToken, o.preferredIP, o.preferredPort)
	if err != nil {
		return nil, fmt.Errorf("建立 stream WS 失败: %w", err)
	}

	stream := &WSStream{
		ws:         ws,
		readCh:     make(chan []byte, 64),
		closed:     make(chan struct{}),
		remoteIP:   targetIP,
		remotePort: targetPort,
	}

	ws.onBinary = func(data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		select {
		case stream.readCh <- cp:
		case <-stream.closed:
		default:
		}
	}

	ws.onMessage = func(msg map[string]interface{}) {
		if msg["type"] == "error" {
			errMsg, _ := msg["error"].(string)
			log.Printf("[WSStream] 服务端错误: %s", errMsg)
			stream.Close()
		}
	}

	ip4 := net.ParseIP(targetIP).To4()
	if ip4 == nil {
		ws.Close()
		return nil, fmt.Errorf("仅支持 IPv4: %s", targetIP)
	}

	handshake := make([]byte, 7)
	handshake[0] = 0x01
	copy(handshake[1:5], ip4)
	binary.BigEndian.PutUint16(handshake[5:7], uint16(targetPort))

	if err := ws.SendBinary(handshake); err != nil {
		ws.Close()
		return nil, fmt.Errorf("发送握手失败: %w", err)
	}

	log.Printf("[WSStream] 建立 %s:%d", targetIP, targetPort)
	return stream, nil
}

func (o *WSOutbound) Close() {
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return
	}
	o.stopped = true
	o.mu.Unlock()
	if o.ws != nil {
		o.ws.Close()
	}
}

type WSStream struct {
	ws         *WSTransport
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
	if err := s.ws.SendBinary(p); err != nil {
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
