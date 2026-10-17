package internal

// WS 应用层隧道（app_stream 二进制版）。
//
// 封包格式：
// | Magic (4B: N2NW) | Type (1B) | PeerIDLen (1B) | PeerID (变长) | StreamID (4B) | PayloadLen (2B) | Payload (变长) |
//
// Type:
//   1 = OPEN     Payload 为 2 字节大端 uint16 目标端口
//   2 = OPEN_ACK Payload 为 1 字节 (1=成功, 0=失败)
//   3 = DATA     Payload 为实际的 TCP 数据
//   4 = CLOSE    Payload 为 reason 字符串

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	streamMagic       = "N2NW"
	streamTypeOpen    = 1
	streamTypeOpenAck = 2
	streamTypeData    = 3
	streamTypeClose   = 4
)

type AppStream struct {
	ID        string
	PeerID    string
	LocalConn net.Conn
	CreatedAt time.Time

	ackCh   chan bool
	ackOnce sync.Once
	closed  atomic.Bool
}

func (s *AppStream) WaitAck(timeout time.Duration) bool {
	select {
	case ok := <-s.ackCh:
		return ok
	case <-time.After(timeout):
		return false
	}
}

func (s *AppStream) signalAck(ok bool) {
	s.ackOnce.Do(func() {
		select {
		case s.ackCh <- ok:
		default:
		}
	})
}

type AppStreamManager struct {
	mu      sync.RWMutex
	streams map[string]*AppStream
	seq     atomic.Uint64
}

func NewAppStreamManager() *AppStreamManager {
	return &AppStreamManager{
		streams: make(map[string]*AppStream),
	}
}

func (m *AppStreamManager) allocID(prefix string) string {
	n := m.seq.Add(1)
	return fmt.Sprintf("%s-%d", prefix, n)
}

func (m *AppStreamManager) add(s *AppStream) {
	m.mu.Lock()
	m.streams[s.ID] = s
	m.mu.Unlock()
}

func (m *AppStreamManager) get(id string) *AppStream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streams[id]
}

func (m *AppStreamManager) remove(id string) *AppStream {
	m.mu.Lock()
	s := m.streams[id]
	delete(m.streams, id)
	m.mu.Unlock()
	return s
}

func (m *AppStreamManager) closeAll() {
	m.mu.Lock()
	for _, s := range m.streams {
		s.closed.Store(true)
		if s.LocalConn != nil {
			_ = s.LocalConn.Close()
		}
	}
	m.streams = make(map[string]*AppStream)
	m.mu.Unlock()
}

// ============ 协议编解码 ============

func encodeStreamPacket(typ uint8, peerID string, streamID string, payload []byte) []byte {
	peerLen := len(peerID)
	
	var sID uint32
	for i := len(streamID) - 1; i >= 0; i-- {
		if streamID[i] < '0' || streamID[i] > '9' {
			break
		}
		sID = sID*10 + uint32(streamID[i]-'0')
	}

	totalLen := 4 + 1 + 1 + peerLen + 4 + 2 + len(payload)
	buf := make([]byte, totalLen)
	copy(buf[0:4], streamMagic)
	buf[4] = typ
	buf[5] = byte(peerLen)
	copy(buf[6:6+peerLen], peerID)
	
	offset := 6 + peerLen
	binary.BigEndian.PutUint32(buf[offset:offset+4], sID)
	offset += 4
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(payload)))
	offset += 2
	copy(buf[offset:], payload)
	
	return buf
}

// handleAppStreamBinary 解析收到的 N2NW 包
func (e *Edge) handleAppStreamBinary(data []byte) {
	if len(data) < 6 || string(data[:4]) != streamMagic {
		return
	}
	typ := data[4]
	peerLen := int(data[5])
	if len(data) < 6+peerLen+4+2 {
		return
	}
	
	from := string(data[6 : 6+peerLen])
	offset := 6 + peerLen
	sID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	payloadLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	
	if len(data) < offset+payloadLen {
		return
	}
	payload := data[offset : offset+payloadLen]
	streamID := fmt.Sprintf("%d", sID)

	switch typ {
	case streamTypeOpen:
		if payloadLen >= 2 {
			port := int(binary.BigEndian.Uint16(payload[:2]))
			e.onRemoteStreamOpenBinary(from, streamID, port)
		}
	case streamTypeOpenAck:
		ok := false
		if payloadLen >= 1 && payload[0] == 1 {
			ok = true
		}
		if s := e.streamMgr.get(streamID); s != nil {
			s.signalAck(ok)
		}
	case streamTypeData:
		if s := e.streamMgr.get(streamID); s != nil && !s.closed.Load() {
			if _, err := s.LocalConn.Write(payload); err != nil {
				log.Printf("[STREAM] %s 本地写失败: %v", streamID, err)
				e.closeStream(s, "local-write-fail")
			}
		}
	case streamTypeClose:
		reason := string(payload)
		if s := e.streamMgr.remove(streamID); s != nil {
			if s.closed.CompareAndSwap(false, true) {
				if s.LocalConn != nil {
					_ = s.LocalConn.Close()
				}
				log.Printf("[STREAM] %s 对端关闭 (%s)", streamID, reason)
			}
		}
	}
}

// ============ Edge 端方法 ============

func (e *Edge) openLocalStream(peerID string, targetPort int, conn net.Conn) (*AppStream, error) {
	if e.streamMgr == nil {
		return nil, fmt.Errorf("stream manager not initialized")
	}
	streamID := e.streamMgr.allocID(e.clientId)
	s := &AppStream{
		ID:        streamID,
		PeerID:    peerID,
		LocalConn: conn,
		CreatedAt: time.Now(),
		ackCh:     make(chan bool, 1),
	}
	e.streamMgr.add(s)

	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, uint16(targetPort))
	pkt := encodeStreamPacket(streamTypeOpen, peerID, streamID, portBuf)

	if err := e.ws.SendBinary(pkt); err != nil {
		e.streamMgr.remove(streamID)
		return nil, err
	}
	log.Printf("[STREAM] %s → %s:%d 打开", streamID, peerID, targetPort)
	return s, nil
}

func (e *Edge) pumpLocalToRemote(s *AppStream) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[STREAM] pumpLocalToRemote panic: %v", r)
		}
	}()

	buf := make([]byte, 8192)
	for {
		if s.closed.Load() {
			return
		}
		n, err := s.LocalConn.Read(buf)
		if n > 0 {
			pkt := encodeStreamPacket(streamTypeData, s.PeerID, s.ID, buf[:n])
			_ = e.ws.SendBinary(pkt)
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("[STREAM] %s 本地读错误: %v", s.ID, err)
			}
			e.closeStream(s, "local-eof")
			return
		}
	}
}

func (e *Edge) closeStream(s *AppStream, reason string) {
	if s == nil {
		return
	}
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	if e.streamMgr != nil {
		e.streamMgr.remove(s.ID)
	}
	if s.LocalConn != nil {
		_ = s.LocalConn.Close()
	}
	if e.ws != nil {
		pkt := encodeStreamPacket(streamTypeClose, s.PeerID, s.ID, []byte(reason))
		_ = e.ws.SendBinary(pkt)
	}
	log.Printf("[STREAM] %s 关闭 (%s)", s.ID, reason)
}

// ============ 接收端处理 ============

func (e *Edge) onRemoteStreamOpenBinary(from string, streamID string, targetPort int) {
	if from == "" || streamID == "" || targetPort <= 0 || targetPort > 65535 {
		return
	}
	if e.streamMgr == nil {
		return
	}

	log.Printf("[STREAM] 收到 open %s from %s targetPort=%d", streamID, from, targetPort)

	addr := fmt.Sprintf("127.0.0.1:%d", targetPort)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		log.Printf("[STREAM] %s dial %s 失败: %v", streamID, addr, err)
		pkt := encodeStreamPacket(streamTypeOpenAck, from, streamID, []byte{0})
		_ = e.ws.SendBinary(pkt)
		return
	}

	s := &AppStream{
		ID:        streamID,
		PeerID:    from,
		LocalConn: conn,
		CreatedAt: time.Now(),
		ackCh:     make(chan bool, 1),
	}
	e.streamMgr.add(s)

	pkt := encodeStreamPacket(streamTypeOpenAck, from, streamID, []byte{1})
	_ = e.ws.SendBinary(pkt)

	safeGo("app-stream-pump", func() { e.pumpLocalToRemote(s) })
}
