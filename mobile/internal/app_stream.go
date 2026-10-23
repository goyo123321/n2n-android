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
//
// ★ 新增：N2NA 二进制数据面（与文本 app_stream_data 并存）
//
//   app_stream_open / open_ack / close 保持文本（低频，无需优化）
//   app_stream_data 在双方 features 都含 "n2na" 时走 N2NA 二进制
//   否则回退文本 base64（完全向后兼容）

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

	// ★ 新增：对端是否支持 N2NA 二进制数据面
	PeerSupportsN2NA bool

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

// allocID 分配 streamId。
//
// ★ 加时间戳：客户端崩溃重启后 seq 重置，旧 streamId 可能
//   在对端还存活，导致新数据被写入旧流。加时间戳后每个 ID 全局唯一。
func (m *AppStreamManager) allocID(prefix string) string {
	n := m.seq.Add(1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixMilli(), n)
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

// ============ 对端 features 查询辅助 ============

func (e *Edge) peerSupportsN2NA(peerID string) bool {
	if peerID == "" {
		return false
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	p := e.peers[peerID]
	if p == nil {
		return false
	}
	for _, f := range p.Features {
		if f == "n2na" {
			return true
		}
	}
	return false
}

// ============ Edge 端方法 ============

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

	supportsBinary := e.peerSupportsN2NA(peerID)

	streamID := e.streamMgr.allocID(e.clientId)
	s := &AppStream{
		ID:               streamID,
		PeerID:           peerID,
		LocalConn:        conn,
		CreatedAt:        time.Now(),
		PeerSupportsN2NA: supportsBinary,
		ackCh:            make(chan bool, 1),
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
	log.Printf("[STREAM] %s → %s:%d 打开 (binary=%v)", streamID, peerID, targetPort, supportsBinary)
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
			if s.PeerSupportsN2NA {
				frame := buildN2NAFrame(s.PeerID, s.ID, buf[:n])
				if frame != nil {
					if sendErr := e.ws.SendBinary(frame); sendErr != nil {
						dataB64 := base64.StdEncoding.EncodeToString(buf[:n])
						_ = e.ws.Send(map[string]interface{}{
							"type":     "app_stream_data",
							"to":       s.PeerID,
							"streamId": s.ID,
							"data":     dataB64,
						})
					}
				} else {
					dataB64 := base64.StdEncoding.EncodeToString(buf[:n])
					_ = e.ws.Send(map[string]interface{}{
						"type":     "app_stream_data",
						"to":       s.PeerID,
						"streamId": s.ID,
						"data":     dataB64,
					})
				}
			} else {
				dataB64 := base64.StdEncoding.EncodeToString(buf[:n])
				_ = e.ws.Send(map[string]interface{}{
					"type":     "app_stream_data",
					"to":       s.PeerID,
					"streamId": s.ID,
					"data":     dataB64,
				})
			}
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

	supportsBinary := e.peerSupportsN2NA(from)

	log.Printf("[STREAM] 收到 open %s from %s targetPort=%d (peerN2NA=%v)",
		streamID, from, targetPort, supportsBinary)

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
		ID:               streamID,
		PeerID:           from,
		LocalConn:        conn,
		CreatedAt:        time.Now(),
		PeerSupportsN2NA: supportsBinary,
		ackCh:            make(chan bool, 1),
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

// ============ N2NA 二进制帧 ============

const (
	n2naHdrMin    = 7
	n2naPeerMax   = 255
	n2naStreamMax = 255
)

func buildN2NAFrame(targetID, streamID string, payload []byte) []byte {
	peerBytes := []byte(targetID)
	streamBytes := []byte(streamID)
	if len(peerBytes) == 0 || len(peerBytes) > n2naPeerMax {
		return nil
	}
	if len(streamBytes) == 0 || len(streamBytes) > n2naStreamMax {
		return nil
	}

	frame := make([]byte, n2naHdrMin+len(peerBytes)+len(streamBytes)+len(payload))
	frame[0] = 'N'
	frame[1] = '2'
	frame[2] = 'N'
	frame[3] = 'A'
	frame[4] = 0
	frame[5] = byte(len(peerBytes))
	copy(frame[6:], peerBytes)
	frame[6+len(peerBytes)] = byte(len(streamBytes))
	copy(frame[7+len(peerBytes):], streamBytes)
	copy(frame[7+len(peerBytes)+len(streamBytes):], payload)
	return frame
}

func isN2NAFrame(data []byte) bool {
	return len(data) >= n2naHdrMin &&
		data[0] == 'N' && data[1] == '2' && data[2] == 'N' && data[3] == 'A'
}

func (e *Edge) handleN2NAFrame(data []byte) {
	if len(data) < n2naHdrMin {
		return
	}
	if data[0] != 'N' || data[1] != '2' || data[2] != 'N' || data[3] != 'A' {
		return
	}

	peerLen := int(data[5])
	if peerLen <= 0 || peerLen > n2naPeerMax {
		return
	}
	if len(data) < 6+peerLen+1 {
		return
	}

	senderID := string(data[6 : 6+peerLen])
	streamIdLen := int(data[6+peerLen])
	if streamIdLen <= 0 || streamIdLen > n2naStreamMax {
		return
	}
	if len(data) < 7+peerLen+streamIdLen {
		return
	}

	streamID := string(data[7+peerLen : 7+peerLen+streamIdLen])
	payload := data[7+peerLen+streamIdLen:]

	now := time.Now().UnixMilli()
	e.peersMu.Lock()
	if p, ok := e.peers[senderID]; ok {
		p.lastRecvAt = now
	}
	e.peersMu.Unlock()

	if e.streamMgr == nil {
		return
	}
	s := e.streamMgr.get(streamID)
	if s == nil || s.closed.Load() {
		return
	}

	if len(payload) > 0 {
		if _, err := s.LocalConn.Write(payload); err != nil {
			log.Printf("[STREAM] %s N2NA 本地写失败: %v", streamID, err)
			e.closeStream(s, "local-write-fail")
		}
	}
}
