package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

var p2pMagic = [4]byte{'N', '2', 'N', 'S'}

const (
	p2pTypeSYN    = 1
	p2pTypeSYNACK = 2
	p2pTypeDATA   = 3
	p2pTypeACK    = 4
	p2pTypeFIN    = 5
	p2pTypeRST    = 6
	p2pTypePING   = 7
	p2pTypePONG   = 8
)

const (
	p2pVer         = 1
	p2pHeaderLen   = 21
	p2pMaxPayload  = 1200
	p2pWindow      = 16
	p2pResendMs    = 200
	p2pMaxRetries  = 8
	p2pIdleTimeout = 120 * time.Second
)

type p2pFrame struct {
	Type     uint8
	StreamID uint32
	Seq      uint32
	Ack      uint32
	Flags    uint8
	Payload  []byte
}

func encodeP2PFrame(f *p2pFrame) []byte {
	buf := make([]byte, p2pHeaderLen+len(f.Payload))
	copy(buf[0:4], p2pMagic[:])
	buf[4] = p2pVer
	buf[5] = f.Type
	binary.BigEndian.PutUint32(buf[6:10], f.StreamID)
	binary.BigEndian.PutUint32(buf[10:14], f.Seq)
	binary.BigEndian.PutUint32(buf[14:18], f.Ack)
	buf[18] = f.Flags
	binary.BigEndian.PutUint16(buf[19:21], uint16(len(f.Payload)))
	if len(f.Payload) > 0 {
		copy(buf[21:], f.Payload)
	}
	return buf
}

func decodeP2PFrame(data []byte) (*p2pFrame, error) {
	if len(data) < p2pHeaderLen {
		return nil, errors.New("short")
	}
	if data[0] != 'N' || data[1] != '2' || data[2] != 'N' || data[3] != 'S' {
		return nil, errors.New("magic")
	}
	if data[4] != p2pVer {
		return nil, errors.New("ver")
	}
	plen := int(binary.BigEndian.Uint16(data[19:21]))
	if len(data) < p2pHeaderLen+plen {
		return nil, errors.New("trunc")
	}
	return &p2pFrame{
		Type:     data[5],
		StreamID: binary.BigEndian.Uint32(data[6:10]),
		Seq:      binary.BigEndian.Uint32(data[10:14]),
		Ack:      binary.BigEndian.Uint32(data[14:18]),
		Flags:    data[18],
		Payload:  data[21 : 21+plen],
	}, nil
}

func isP2PFrame(data []byte) bool {
	return len(data) >= 4 && data[0] == 'N' && data[1] == '2' && data[2] == 'N' && data[3] == 'S'
}

type p2pSeg struct {
	seq      uint32
	data     []byte
	sentAt   time.Time
	retries  int
	isSyn    bool
	isSynAck bool
}

type P2PStream struct {
	ID     uint32
	PeerID string

	mu       sync.Mutex
	peerAddr *net.UDPAddr
	viaTURN  bool

	lastRecvFrom *net.UDPAddr

	sendNext uint32
	sendBase uint32
	segments map[uint32]*p2pSeg

	recvNext uint32
	recvBuf  map[uint32][]byte

	// 最近 5 秒交付过的 seq，用于消除多目标发送导致的 DUP
	recentlyDelivered map[uint32]time.Time

	localConn net.Conn
	ownsConn  bool

	closed        bool
	closeCh       chan struct{}
	establishedCh chan bool
	lastAct       time.Time
}

type P2PStreamManager struct {
	mu   sync.RWMutex
	byID map[uint32]*P2PStream
	next uint32
	edge *Edge
}

func NewP2PStreamManager(edge *Edge) *P2PStreamManager {
	m := &P2PStreamManager{byID: make(map[uint32]*P2PStream), next: 2, edge: edge}
	safeGo("p2p-reaper", m.reaper)
	return m
}

func (m *P2PStreamManager) allocID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		id := m.next
		m.next += 2
		if m.next == 0 {
			m.next = 2
		}
		if _, dup := m.byID[id]; !dup {
			return id
		}
	}
}

func (m *P2PStreamManager) add(s *P2PStream) {
	m.mu.Lock()
	m.byID[s.ID] = s
	m.mu.Unlock()
}

func (m *P2PStreamManager) get(id uint32) *P2PStream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byID[id]
}

func (m *P2PStreamManager) remove(id uint32) {
	m.mu.Lock()
	delete(m.byID, id)
	m.mu.Unlock()
}

func (m *P2PStreamManager) closeAll() {
	m.mu.Lock()
	all := make([]*P2PStream, 0, len(m.byID))
	for _, s := range m.byID {
		all = append(all, s)
	}
	m.byID = make(map[uint32]*P2PStream)
	m.mu.Unlock()
	for _, s := range all {
		s.close("manager-shutdown")
	}
}

func (m *P2PStreamManager) dropPeer(peerID string) {
	if m == nil || peerID == "" {
		return
	}
	m.mu.RLock()
	var toClose []*P2PStream
	for _, s := range m.byID {
		if s.PeerID == peerID {
			toClose = append(toClose, s)
		}
	}
	m.mu.RUnlock()
	for _, s := range toClose {
		s.close("peer-left")
	}
}

func (m *P2PStreamManager) reaper() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		m.mu.RLock()
		var stale []*P2PStream
		for _, s := range m.byID {
			s.mu.Lock()
			idle := now.Sub(s.lastAct)
			s.mu.Unlock()
			if idle > p2pIdleTimeout {
				stale = append(stale, s)
			}
		}
		m.mu.RUnlock()
		for _, s := range stale {
			s.close("idle")
		}
	}
}

// sendP2PProbe 向 peerID 发一个裸 P2P PING 帧（无需 stream）。
func (m *P2PStreamManager) sendP2PProbe(peerID string) {
	if m == nil || m.edge == nil || peerID == "" {
		return
	}
	addr, viaTURN := m.edge.peerAddrAndMode(peerID)
	if addr == nil {
		return
	}
	frame := &p2pFrame{
		Type:     p2pTypePING,
		StreamID: 0,
	}
	tmp := &P2PStream{
		ID:       0,
		PeerID:   peerID,
		peerAddr: addr,
		viaTURN:  viaTURN,
	}
	if err := m.edge.sendP2PFrame(tmp, frame); err != nil {
		log.Printf("[P2P-PROBE] 向 %s 发 P2P PING 失败: %v", peerID, err)
	}
}

// collectN2NSTargets 收集一个 stream 的所有候选发送地址。
//
// 优先级（保持顺序，去重保留首次）：
//   1. LAN 地址（同 WiFi 场景）
//   2. AssistedEndpoints（STUN 反射的多公网映射）
//   3. lastRecvFrom（对端最近一次发来的源地址，最可信）
//   4. peerAddr（注册时分配的地址 / SYN 来源地址）
//   5. peer.UDPAddr（观测 raddr）
func (e *Edge) collectN2NSTargets(s *P2PStream) []*net.UDPAddr {
	s.mu.Lock()
	primary := s.peerAddr
	lastRecv := s.lastRecvFrom
	peerID := s.PeerID
	s.mu.Unlock()

	e.peersMu.RLock()
	var lanEndpoints []string
	var assisted []string
	var peerUDPAddr *net.UDPAddr
	if p := e.peers[peerID]; p != nil {
		lanEndpoints = p.LanEndpoints
		assisted = p.AssistedEndpoints
		peerUDPAddr = p.UDPAddr
	}
	e.peersMu.RUnlock()

	var out []*net.UDPAddr
	seen := make(map[string]bool)
	add := func(a *net.UDPAddr) {
		if a == nil || a.IP == nil {
			return
		}
		k := a.String()
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, a)
	}

	for _, lan := range lanEndpoints {
		if ra, err := net.ResolveUDPAddr("udp", lan); err == nil {
			add(ra)
		}
	}
	for _, ep := range assisted {
		if ra, err := net.ResolveUDPAddr("udp", ep); err == nil {
			add(ra)
		}
	}
	add(lastRecv)
	add(primary)
	add(peerUDPAddr)

	return out
}

// sendP2PFrame 发送一帧到对端的所有候选地址。
//
// ★ Android 特有：e.udpConn 是 net.PacketConn（不是 *net.UDPConn），
//   用 WriteTo(addr) 而非 WriteToUDP。
func (e *Edge) sendP2PFrame(s *P2PStream, f *p2pFrame) error {
	s.mu.Lock()
	viaTURN := s.viaTURN
	peerID := s.PeerID
	s.mu.Unlock()

	buf := encodeP2PFrame(f)

	if viaTURN {
		if e.turnClient == nil || !e.turnClient.IsReady() {
			return errors.New("turn down")
		}
		e.peersMu.RLock()
		var relayAddr string
		if p := e.peers[peerID]; p != nil {
			relayAddr = p.TurnRelayAddr
		}
		e.peersMu.RUnlock()

		if relayAddr == "" {
			return errors.New("peer turn relay unknown")
		}
		ra, err := net.ResolveUDPAddr("udp", relayAddr)
		if err != nil {
			return fmt.Errorf("bad relay addr %q: %w", relayAddr, err)
		}
		return e.turnClient.Send(buf, ra)
	}

	targets := e.collectN2NSTargets(s)
	if len(targets) == 0 {
		return errors.New("no targets for N2NS frame")
	}
	if e.udpConn == nil {
		return errors.New("no udp")
	}

	var lastErr error
	sent := 0
	for _, addr := range targets {
		if _, err := e.udpConn.WriteTo(buf, addr); err != nil {
			lastErr = err
		} else {
			sent++
		}
	}
	if sent == 0 {
		if lastErr == nil {
			lastErr = errors.New("all sends failed")
		}
		return lastErr
	}
	return nil
}

func (m *P2PStreamManager) OpenP2PStream(peerID string, targetPort int, localConn net.Conn, timeout time.Duration) (*P2PStream, error) {
	addr, viaTURN := m.edge.peerAddrAndMode(peerID)
	if addr == nil {
		return nil, errors.New("peer addr unknown")
	}
	s := &P2PStream{
		ID:                m.allocID(),
		PeerID:            peerID,
		peerAddr:          addr,
		viaTURN:           viaTURN,
		segments:          make(map[uint32]*p2pSeg),
		recvBuf:           make(map[uint32][]byte),
		recentlyDelivered: make(map[uint32]time.Time),
		localConn:         localConn,
		ownsConn:          false,
		closeCh:           make(chan struct{}),
		establishedCh:     make(chan bool, 1),
		lastAct:           time.Now(),
	}
	m.add(s)

	portBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(portBuf, uint32(targetPort))
	syn := &p2pFrame{Type: p2pTypeSYN, StreamID: s.ID, Seq: 0, Payload: portBuf}

	s.mu.Lock()
	s.segments[0] = &p2pSeg{seq: 0, sentAt: time.Now(), isSyn: true}
	s.sendNext = 1
	s.recvNext = 1
	s.mu.Unlock()

	if err := m.edge.sendP2PFrame(s, syn); err != nil {
		m.remove(s.ID)
		s.abortNoConn()
		return nil, err
	}

	safeGo("p2p-rtx", func() { s.retransmitLoop(m.edge) })

	select {
	case ok := <-s.establishedCh:
		if !ok {
			m.remove(s.ID)
			s.abortNoConn()
			return nil, errors.New("peer rejected")
		}
		s.mu.Lock()
		s.ownsConn = true
		s.mu.Unlock()
		safeGo("p2p-tx", func() { s.readLocalAndSend(m.edge) })
		return s, nil

	case <-time.After(timeout):
		m.remove(s.ID)
		log.Printf("[P2P-STREAM] %d 建流超时（%v），保留 conn 让调用方回落", s.ID, timeout)
		s.abortNoConn()
		return nil, errors.New("syn timeout")

	case <-s.closeCh:
		m.remove(s.ID)
		s.mu.Lock()
		s.localConn = nil
		s.closed = true
		s.mu.Unlock()
		return nil, errors.New("closed")
	}
}

func (s *P2PStream) abortNoConn() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.localConn = nil
	s.mu.Unlock()
	close(s.closeCh)
}

// acceptSYN 处理收到的 SYN 帧。幂等：多目标发送会导致同一 SYN 到达多次。
func (m *P2PStreamManager) acceptSYN(peerID string, peerAddr *net.UDPAddr, viaTURN bool, f *p2pFrame) {
	if len(f.Payload) < 4 {
		return
	}
	targetPort := int(binary.BigEndian.Uint32(f.Payload[:4]))
	if targetPort <= 0 || targetPort > 65535 {
		return
	}

	if existing := m.get(f.StreamID); existing != nil {
		existing.mu.Lock()
		existing.lastRecvFrom = peerAddr
		existing.mu.Unlock()
		synack := &p2pFrame{
			Type:     p2pTypeSYNACK,
			StreamID: f.StreamID,
			Seq:      0,
			Ack:      1,
		}
		_ = m.edge.sendP2PFrame(existing, synack)
		return
	}

	localConn, err := net.DialTimeout(
		"tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(targetPort)),
		5*time.Second,
	)
	if err != nil {
		rst := &p2pFrame{Type: p2pTypeRST, StreamID: f.StreamID}
		tmp := &P2PStream{ID: f.StreamID, peerAddr: peerAddr, viaTURN: viaTURN}
		_ = m.edge.sendP2PFrame(tmp, rst)
		log.Printf("[P2P-STREAM] %d dial 127.0.0.1:%d 失败: %v", f.StreamID, targetPort, err)
		return
	}

	s := &P2PStream{
		ID:                f.StreamID,
		PeerID:            peerID,
		peerAddr:          peerAddr,
		viaTURN:           viaTURN,
		lastRecvFrom:      peerAddr,
		segments:          make(map[uint32]*p2pSeg),
		recvBuf:           make(map[uint32][]byte),
		recentlyDelivered: make(map[uint32]time.Time),
		localConn:         localConn,
		ownsConn:          true,
		closeCh:           make(chan struct{}),
		establishedCh:     make(chan bool, 1),
		lastAct:           time.Now(),
	}
	s.establishedCh <- true

	m.mu.Lock()
	if _, dup := m.byID[s.ID]; dup {
		m.mu.Unlock()
		_ = localConn.Close()
		return
	}
	m.byID[s.ID] = s
	m.mu.Unlock()

	s.mu.Lock()
	s.segments[0] = &p2pSeg{seq: 0, sentAt: time.Now(), isSyn: true, isSynAck: true}
	s.sendNext = 1
	s.recvNext = 1
	s.mu.Unlock()

	synack := &p2pFrame{
		Type:     p2pTypeSYNACK,
		StreamID: s.ID,
		Seq:      0,
		Ack:      1,
	}
	_ = m.edge.sendP2PFrame(s, synack)

	log.Printf("[P2P-STREAM] %d 收到 SYN from %s, 转发到 127.0.0.1:%d",
		s.ID, peerID, targetPort)

	safeGo("p2p-rtx", func() { s.retransmitLoop(m.edge) })
	safeGo("p2p-tx", func() { s.readLocalAndSend(m.edge) })
}

func (m *P2PStreamManager) handleFrame(peerID string, peerAddr *net.UDPAddr, viaTURN bool, f *p2pFrame) {
	s := m.get(f.StreamID)

	if s == nil {
		if f.Type == p2pTypePING {
			safeGo("p2p-pong", func() {
				resp := &p2pFrame{Type: p2pTypePONG, StreamID: 0}
				tmp := &P2PStream{
					ID:       0,
					PeerID:   peerID,
					peerAddr: peerAddr,
					viaTURN:  viaTURN,
				}
				_ = m.edge.sendP2PFrame(tmp, resp)
			})
			return
		}
		if f.Type == p2pTypePONG {
			return
		}
		if f.Type == p2pTypeSYN {
			safeGo("p2p-accept", func() { m.acceptSYN(peerID, peerAddr, viaTURN, f) })
			return
		}
		return
	}

	s.mu.Lock()
	s.peerAddr = peerAddr
	s.lastRecvFrom = peerAddr
	s.viaTURN = viaTURN
	s.lastAct = time.Now()

	if f.Type == p2pTypeACK || f.Type == p2pTypeDATA || f.Type == p2pTypeSYNACK {
		if f.Ack > s.sendBase {
			for seq := s.sendBase; seq < f.Ack; seq++ {
				delete(s.segments, seq)
			}
			s.sendBase = f.Ack
		}
	}
	s.mu.Unlock()

	switch f.Type {
	case p2pTypeSYNACK:
		_ = m.edge.sendP2PFrame(s, &p2pFrame{
			Type:     p2pTypeACK,
			StreamID: s.ID,
			Ack:      1,
		})
		select {
		case s.establishedCh <- true:
		default:
		}
	case p2pTypeDATA:
		s.handleData(m.edge, f)
	case p2pTypeACK:
	case p2pTypeFIN, p2pTypeRST:
		s.close("peer-close")
	}
}

// handleData 处理 DATA 帧。
//
// 两级去重：
//   1. f.Seq < recvNext → 已交付过的老帧，直接回 ACK
//   2. recentlyDelivered 里 5 秒内交付过的 seq → 丢弃（防多路径重复）
//
// DUP 根源：sendP2PFrame 多目标发送时，对端可能从 LAN + 公网两条路径
// 都收到同一个 DATA 帧。如果两份到达间隔极短（< 1ms），第一份交付
// 后 recvNext++ 还没生效，第二份也会被判定为 f.Seq == recvNext →
// 重复交付 → 上游 TCP 栈看到重复数据 → ping 显示 DUP。
func (s *P2PStream) handleData(e *Edge, f *p2pFrame) {
	var toWrite [][]byte

	s.mu.Lock()

	// ---- 去重 1：老帧 ----
	if f.Seq < s.recvNext {
		ackToSend := s.recvNext
		s.mu.Unlock()
		_ = e.sendP2PFrame(s, &p2pFrame{
			Type:     p2pTypeACK,
			StreamID: s.ID,
			Ack:      ackToSend,
		})
		return
	}

	// ---- 去重 2：多路径重复 ----
	if s.recentlyDelivered == nil {
		s.recentlyDelivered = make(map[uint32]time.Time)
	}
	if ts, seen := s.recentlyDelivered[f.Seq]; seen && time.Since(ts) < 5*time.Second {
		ackToSend := s.recvNext
		s.mu.Unlock()
		_ = e.sendP2PFrame(s, &p2pFrame{
			Type:     p2pTypeACK,
			StreamID: s.ID,
			Ack:      ackToSend,
		})
		return
	}

	if f.Seq == s.recvNext {
		if len(f.Payload) > 0 {
			cp := make([]byte, len(f.Payload))
			copy(cp, f.Payload)
			toWrite = append(toWrite, cp)
		}
		s.recentlyDelivered[f.Seq] = time.Now()
		s.recvNext++

		for {
			d, ok := s.recvBuf[s.recvNext]
			if !ok {
				break
			}
			delete(s.recvBuf, s.recvNext)
			toWrite = append(toWrite, d)
			s.recentlyDelivered[s.recvNext] = time.Now()
			s.recvNext++
		}

		if len(s.recentlyDelivered) > 100 {
			for seq, ts := range s.recentlyDelivered {
				if time.Since(ts) > 5*time.Second {
					delete(s.recentlyDelivered, seq)
				}
			}
		}
	} else if f.Seq > s.recvNext {
		if _, exists := s.recvBuf[f.Seq]; !exists {
			cp := make([]byte, len(f.Payload))
			copy(cp, f.Payload)
			s.recvBuf[f.Seq] = cp
		}
	}
	ackToSend := s.recvNext
	local := s.localConn
	s.mu.Unlock()

	for _, d := range toWrite {
		if local != nil {
			_, _ = local.Write(d)
		}
	}

	_ = e.sendP2PFrame(s, &p2pFrame{
		Type:     p2pTypeACK,
		StreamID: s.ID,
		Ack:      ackToSend,
	})
}

func (s *P2PStream) readLocalAndSend(e *Edge) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[P2P-STREAM] %d readLocal panic: %v", s.ID, r)
		}
	}()

	buf := make([]byte, p2pMaxPayload)
	for {
		s.mu.Lock()
		closed := s.closed
		local := s.localConn
		s.mu.Unlock()
		if closed || local == nil {
			return
		}

		for {
			s.mu.Lock()
			inflight := s.sendNext - s.sendBase
			s.mu.Unlock()
			if inflight < p2pWindow {
				break
			}
			select {
			case <-s.closeCh:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}

		n, err := local.Read(buf)
		if n > 0 {
			s.mu.Lock()
			seq := s.sendNext
			s.sendNext++
			data := make([]byte, n)
			copy(data, buf[:n])
			s.segments[seq] = &p2pSeg{seq: seq, data: data, sentAt: time.Now()}
			ack := s.recvNext
			s.mu.Unlock()

			_ = e.sendP2PFrame(s, &p2pFrame{
				Type:     p2pTypeDATA,
				StreamID: s.ID,
				Seq:      seq,
				Ack:      ack,
				Payload:  data,
			})
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("[P2P-STREAM] %d local read: %v", s.ID, err)
			}
			s.mu.Lock()
			finSeq := s.sendNext
			s.sendNext++
			s.mu.Unlock()
			_ = e.sendP2PFrame(s, &p2pFrame{Type: p2pTypeFIN, StreamID: s.ID, Seq: finSeq})
			s.close("local-eof")
			return
		}
	}
}

func (s *P2PStream) retransmitLoop(e *Edge) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[P2P-STREAM] %d retransmit panic: %v", s.ID, r)
		}
	}()
	t := time.NewTicker(p2pResendMs * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-t.C:
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		now := time.Now()
		var toResend []*p2pSeg
		var kill bool
		for seq := s.sendBase; seq < s.sendNext; seq++ {
			seg, ok := s.segments[seq]
			if !ok {
				continue
			}
			if now.Sub(seg.sentAt) < p2pResendMs*time.Millisecond {
				continue
			}
			if seg.retries >= p2pMaxRetries {
				kill = true
				break
			}
			seg.retries++
			seg.sentAt = now
			toResend = append(toResend, seg)
		}
		ack := s.recvNext
		s.mu.Unlock()

		if kill {
			log.Printf("[P2P-STREAM] %d 重传耗尽（对端无响应）", s.ID)
			s.close("retransmit-exhausted")
			return
		}
		for _, seg := range toResend {
			var ft uint8 = p2pTypeDATA
			if seg.isSyn {
				if seg.isSynAck {
					ft = p2pTypeSYNACK
				} else {
					ft = p2pTypeSYN
				}
			}
			_ = e.sendP2PFrame(s, &p2pFrame{
				Type:     ft,
				StreamID: s.ID,
				Seq:      seg.seq,
				Ack:      ack,
				Payload:  seg.data,
			})
		}
	}
}

func (s *P2PStream) close(reason string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	local := s.localConn
	owns := s.ownsConn
	s.localConn = nil
	s.mu.Unlock()

	close(s.closeCh)
	if local != nil && owns {
		_ = local.Close()
	}
	log.Printf("[P2P-STREAM] %d 关闭 (%s)", s.ID, reason)
}
