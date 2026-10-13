package internal

// WS 应用层隧道（app_stream）。
//
// 与 n2n-go-client / n2n-termux 完全一致。
//
// 使用场景：三级降级到 WS 中继时，PC/Termux 主动向 Android 上的
// 服务发起连接。此时数据面无法走 TUN（因为 WS 中继是双向文本协
// 议），改用 app_stream_* 消息把 TCP 字节流封装成 base64 隧道。
//
// 消息：
//   app_stream_open      → 发起方 → 接收方，携带 targetPort
//   app_stream_open_ack  ← 接收方 → 发起方，携带 ok
//   app_stream_data      → 双向，携带 base64 编码的 TCP 数据
//   app_stream_close     → 双向，携带 reason

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
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

// ============ Edge 端方法 ============

// findPeerByVIP 按虚拟 IP 查 peerID。
//
// Android 端当前不通过 SOCKS5 触发应用层流，此方法保留给未来扩展
// （例如"本地代理到虚拟 IP"的场景）。
func (e *Edge) findPeerByVIP(vip string) (string, bool) {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	for id, p := range e.peers {
		if p.VirtualIP == vip {
			return id, true
		}
	}
	return "", false
}

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

	err := e.ws.Send(map[string]interface{}{
		"type":       "app_stream_open",
		"to":         peerID,
		"streamId":   streamID,
		"targetPort": targetPort,
	})
	if err != nil {
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

	buf := make([]byte, 2048)
	for {
		if s.closed.Load() {
			return
		}
		n, err := s.LocalConn.Read(buf)
		if n > 0 {
			dataB64 := base64.StdEncoding.EncodeToString(buf[:n])
			_ = e.ws.Send(map[string]interface{}{
				"type":     "app_stream_data",
				"to":       s.PeerID,
				"streamId": s.ID,
				"data":     dataB64,
			})
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
		_ = e.ws.Send(map[string]interface{}{
			"type":     "app_stream_close",
			"to":       s.PeerID,
			"streamId": s.ID,
			"reason":   reason,
		})
	}
	log.Printf("[STREAM] %s 关闭 (%s)", s.ID, reason)
}

// ============ 远端消息处理 ============

// onRemoteStreamOpen 处理对端发起的应用层隧道。
//
// 本端作为"接收方"：dial 到 127.0.0.1:targetPort，成功回 open_ack=true，
// 然后开始 pump。
func (e *Edge) onRemoteStreamOpen(msg map[string]interface{}) {
	from, _ := msg["from"].(string)
	streamID, _ := msg["streamId"].(string)
	targetPort := 0
	if f, ok := msg["targetPort"].(float64); ok {
		targetPort = int(f)
	}
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
		_ = e.ws.Send(map[string]interface{}{
			"type":     "app_stream_open_ack",
			"to":       from,
			"streamId": streamID,
			"ok":       false,
		})
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

	_ = e.ws.Send(map[string]interface{}{
		"type":     "app_stream_open_ack",
		"to":       from,
		"streamId": streamID,
		"ok":       true,
	})

	safeGo("app-stream-pump", func() { e.pumpLocalToRemote(s) })
}

func (e *Edge) onRemoteStreamAck(msg map[string]interface{}) {
	if e.streamMgr == nil {
		return
	}
	streamID, _ := msg["streamId"].(string)
	ok, _ := msg["ok"].(bool)
	s := e.streamMgr.get(streamID)
	if s == nil {
		return
	}
	s.signalAck(ok)
}

func (e *Edge) onRemoteStreamData(msg map[string]interface{}) {
	if e.streamMgr == nil {
		return
	}
	streamID, _ := msg["streamId"].(string)
	dataB64, _ := msg["data"].(string)
	if streamID == "" || dataB64 == "" {
		return
	}

	s := e.streamMgr.get(streamID)
	if s == nil || s.closed.Load() {
		return
	}

	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		log.Printf("[STREAM] %s base64 解码失败: %v", streamID, err)
		return
	}

	if _, err := s.LocalConn.Write(data); err != nil {
		log.Printf("[STREAM] %s 本地写失败: %v", streamID, err)
		e.closeStream(s, "local-write-fail")
	}
}

func (e *Edge) onRemoteStreamClose(msg map[string]interface{}) {
	if e.streamMgr == nil {
		return
	}
	streamID, _ := msg["streamId"].(string)
	reason, _ := msg["reason"].(string)
	s := e.streamMgr.remove(streamID)
	if s == nil {
		return
	}
	if s.closed.CompareAndSwap(false, true) {
		if s.LocalConn != nil {
			_ = s.LocalConn.Close()
		}
		log.Printf("[STREAM] %s 对端关闭 (%s)", streamID, reason)
	}
}
