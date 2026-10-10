package internal

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const stunMagicCookie = 0x2112A442

const (
	msgAllocateRequest     = 0x0003
	msgAllocateSuccess     = 0x0103
	msgAllocateError       = 0x0113
	msgRefreshRequest      = 0x0004
	msgRefreshSuccess      = 0x0104
	msgCreatePermissionReq = 0x0008
	msgCreatePermissionSuc = 0x0108
	msgSendIndication      = 0x0016
	msgDataIndication      = 0x0017
)

const (
	attrMappedAddress    = 0x0001
	attrUsername         = 0x0006
	attrMessageIntegrity = 0x0008
	attrErrorCode        = 0x0009
	attrLifetime         = 0x000D
	attrXorPeerAddress   = 0x0012
	attrData             = 0x0013
	attrRealm            = 0x0014
	attrNonce            = 0x0015
	attrXorRelayedAddr   = 0x0016
	attrRequestedTransID = 0x0019
	attrXorMappedAddress = 0x0020
)

const transportUDP = 17

const (
	turnPermissionTTL   = 4 * time.Minute
	turnDefaultLifetime = 600
	turnRefreshInterval = 60 * time.Second
	turnRequestTimeout  = 5 * time.Second
)

type stunAttr struct {
	typ   uint16
	value []byte
}

type stunMessage struct {
	msgType uint16
	txid    [12]byte
	attrs   map[uint16][]byte
}

type TURNLite struct {
	serverAddr string
	username   string
	password   string
	useTCP     bool

	realm string
	nonce []byte
	key   []byte

	conn net.Conn

	relayAddr  *net.UDPAddr
	mappedAddr *net.UDPAddr

	permissions map[string]time.Time
	permMu      sync.Mutex

	onMessage func([]byte, net.Addr)

	pendingMu sync.Mutex
	pending   map[[12]byte]chan *stunMessage

	mu       sync.Mutex
	stopped  bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewTURNLite(serverAddr, username, password string) *TURNLite {
	return NewTURNLiteWithTCP(serverAddr, username, password, false)
}

func NewTURNLiteWithTCP(serverAddr, username, password string, useTCP bool) *TURNLite {
	return &TURNLite{
		serverAddr:  serverAddr,
		username:    username,
		password:    password,
		useTCP:      useTCP,
		permissions: make(map[string]time.Time),
		pending:     make(map[[12]byte]chan *stunMessage),
		stopCh:      make(chan struct{}),
	}
}

func isNetworkUnreachable(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "network is unreachable") ||
		strings.Contains(s, "no route to host") ||
		strings.Contains(s, "network is down")
}

func (t *TURNLite) IsAlive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.stopped
}

func buildSTUNMsg(msgType uint16, txid [12]byte, attrs []stunAttr) []byte {
	total := 20
	for _, a := range attrs {
		total += 4 + ((len(a.value) + 3) &^ 3)
	}
	buf := make([]byte, total)
	binary.BigEndian.PutUint16(buf[0:2], msgType)
	binary.BigEndian.PutUint16(buf[2:4], uint16(total-20))
	binary.BigEndian.PutUint32(buf[4:8], stunMagicCookie)
	copy(buf[8:20], txid[:])
	offset := 20
	for _, a := range attrs {
		binary.BigEndian.PutUint16(buf[offset:offset+2], a.typ)
		binary.BigEndian.PutUint16(buf[offset+2:offset+4], uint16(len(a.value)))
		copy(buf[offset+4:], a.value)
		offset += 4 + ((len(a.value) + 3) &^ 3)
	}
	return buf
}

func parseSTUNMsg(data []byte) (*stunMessage, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("STUN 消息太短")
	}
	if binary.BigEndian.Uint32(data[4:8]) != stunMagicCookie {
		return nil, fmt.Errorf("magic cookie 不匹配")
	}
	msgLen := int(binary.BigEndian.Uint16(data[2:4]))
	end := 20 + msgLen
	if end > len(data) {
		end = len(data)
	}
	attrs := make(map[uint16][]byte)
	offset := 20
	for offset+4 <= end {
		typ := binary.BigEndian.Uint16(data[offset : offset+2])
		l := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		if offset+4+l > end {
			break
		}
		attrs[typ] = data[offset+4 : offset+4+l]
		offset += 4 + ((l + 3) &^ 3)
	}
	var txid [12]byte
	copy(txid[:], data[8:20])
	return &stunMessage{
		msgType: binary.BigEndian.Uint16(data[0:2]),
		txid:    txid,
		attrs:   attrs,
	}, nil
}

var magicBytes = [4]byte{0x21, 0x12, 0xA4, 0x42}

func xorEncodePeer(ip net.IP, port int) []byte {
	if v4 := ip.To4(); v4 != nil {
		buf := make([]byte, 8)
		buf[1] = 0x01
		binary.BigEndian.PutUint16(buf[2:4], uint16(port)^0x2112)
		for i := 0; i < 4; i++ {
			buf[4+i] = v4[i] ^ magicBytes[i]
		}
		return buf
	}
	if v6 := ip.To16(); v6 != nil {
		buf := make([]byte, 20)
		buf[1] = 0x02
		binary.BigEndian.PutUint16(buf[2:4], uint16(port)^0x2112)
		for i := 0; i < 16; i++ {
			buf[4+i] = v6[i] ^ magicBytes[i%4]
		}
		return buf
	}
	return nil
}

func xorDecodePeer(data []byte) (net.IP, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("XOR 地址太短")
	}
	family := data[1]
	port := int(binary.BigEndian.Uint16(data[2:4]) ^ 0x2112)
	switch family {
	case 0x01:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("IPv4 数据不足")
		}
		ip := make(net.IP, 4)
		for i := 0; i < 4; i++ {
			ip[i] = data[4+i] ^ magicBytes[i]
		}
		return ip, port, nil
	case 0x02:
		if len(data) < 20 {
			return nil, 0, fmt.Errorf("IPv6 数据不足")
		}
		ip := make(net.IP, 16)
		for i := 0; i < 16; i++ {
			ip[i] = data[4+i] ^ magicBytes[i%4]
		}
		return ip, port, nil
	}
	return nil, 0, fmt.Errorf("未知地址族 0x%02x", family)
}

func (t *TURNLite) sign(msg []byte) []byte {
	if t.key == nil {
		return msg
	}

	newBodyLen := len(msg) - 20 + 24

	tmp := make([]byte, len(msg))
	copy(tmp, msg)
	binary.BigEndian.PutUint16(tmp[2:4], uint16(newBodyLen))

	mac := hmac.New(sha1.New, t.key)
	mac.Write(tmp)
	sig := mac.Sum(nil)

	out := make([]byte, len(msg)+24)
	copy(out, msg)
	binary.BigEndian.PutUint16(out[len(msg):len(msg)+2], attrMessageIntegrity)
	binary.BigEndian.PutUint16(out[len(msg)+2:len(msg)+4], 20)
	copy(out[len(msg)+4:], sig)
	binary.BigEndian.PutUint16(out[2:4], uint16(newBodyLen))
	return out
}

func randTxID() [12]byte {
	var t [12]byte
	_, _ = rand.Read(t[:])
	return t
}

func (t *TURNLite) sendRequest(msgType uint16, attrs []stunAttr, withAuth bool) (*stunMessage, error) {
	txid := randTxID()

	if withAuth && t.key != nil {
		attrs = append(attrs,
			stunAttr{typ: attrUsername, value: []byte(t.username)},
			stunAttr{typ: attrRealm, value: []byte(t.realm)},
			stunAttr{typ: attrNonce, value: t.nonce},
		)
	}

	msg := buildSTUNMsg(msgType, txid, attrs)
	if withAuth && t.key != nil {
		msg = t.sign(msg)
	}

	ch := make(chan *stunMessage, 1)
	t.pendingMu.Lock()
	t.pending[txid] = ch
	t.pendingMu.Unlock()
	defer func() {
		t.pendingMu.Lock()
		delete(t.pending, txid)
		t.pendingMu.Unlock()
	}()

	if _, err := t.conn.Write(msg); err != nil {
		if isNetworkUnreachable(err) {
			log.Printf("[TURN-Lite] socket 失效（网络变化），关闭等待重建")
			go t.Close()
		}
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	timeout := time.After(turnRequestTimeout)
	select {
	case resp := <-ch:
		if resp == nil {
			return nil, fmt.Errorf("空响应")
		}
		return resp, nil
	case <-timeout:
		return nil, fmt.Errorf("请求超时 (type=0x%04x)", msgType)
	case <-t.stopCh:
		return nil, fmt.Errorf("已关闭")
	}
}

func (t *TURNLite) dispatch(msg *stunMessage) {
	t.pendingMu.Lock()
	ch := t.pending[msg.txid]
	t.pendingMu.Unlock()
	if ch != nil {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (t *TURNLite) Allocate() error {
	protectedDialer := newProtectedDialer()

	if t.useTCP {
		log.Printf("[TURN-Lite] TCP transport → %s", t.serverAddr)
		conn, err := protectedDialer.Dial("tcp", t.serverAddr)
		if err != nil {
			return fmt.Errorf("TCP 连接失败: %w", err)
		}
		t.conn = conn
	} else {
		log.Printf("[TURN-Lite] UDP transport → %s", t.serverAddr)
		conn, err := protectedDialer.Dial("udp4", t.serverAddr)
		if err != nil {
			return fmt.Errorf("UDP 连接失败: %w", err)
		}
		t.conn = conn
	}

	safeGo("turn-lite-readLoop", t.readLoop)

	reqTransport := []byte{transportUDP, 0, 0, 0}

	log.Printf("[TURN-Lite] 发送初始 Allocate（无认证）")
	resp, err := t.sendRequest(msgAllocateRequest, []stunAttr{
		{typ: attrRequestedTransID, value: reqTransport},
	}, false)
	if err != nil {
		t.Close()
		return fmt.Errorf("初始 Allocate 失败: %w", err)
	}

	if resp.msgType == msgAllocateError {
		realm := string(resp.attrs[attrRealm])
		nonce := resp.attrs[attrNonce]
		if realm == "" || len(nonce) == 0 {
			t.Close()
			return fmt.Errorf("401 缺 realm/nonce")
		}
		t.realm = realm
		t.nonce = nonce

		h := md5.Sum([]byte(fmt.Sprintf("%s:%s:%s", t.username, t.realm, t.password)))
		t.key = h[:]

		log.Printf("[TURN-Lite] 401 realm=%q nonce=%d 字节 (username=%s)", realm, len(nonce), t.username)
	} else if resp.msgType == msgAllocateSuccess {
		return t.extractAllocateResult(resp)
	} else {
		t.Close()
		return fmt.Errorf("初始 Allocate 返回意外类型 0x%04x", resp.msgType)
	}

	log.Printf("[TURN-Lite] 发送 Allocate（带认证）")
	resp, err = t.sendRequest(msgAllocateRequest, []stunAttr{
		{typ: attrRequestedTransID, value: reqTransport},
		{typ: attrLifetime, value: uint32ToBytes(turnDefaultLifetime)},
	}, true)
	if err != nil {
		t.Close()
		return fmt.Errorf("认证 Allocate 失败: %w", err)
	}

	if resp.msgType != msgAllocateSuccess {
		t.Close()
		code := parseErrorCode(resp.attrs[attrErrorCode])
		return fmt.Errorf("Allocate 失败: code=%d", code)
	}

	return t.extractAllocateResult(resp)
}

func (t *TURNLite) extractAllocateResult(resp *stunMessage) error {
	relayData := resp.attrs[attrXorRelayedAddr]
	ip, port, err := xorDecodePeer(relayData)
	if err != nil {
		return fmt.Errorf("解析 relay 失败: %w", err)
	}
	t.relayAddr = &net.UDPAddr{IP: ip, Port: port}

	if m := resp.attrs[attrXorMappedAddress]; m != nil {
		if ip2, port2, err2 := xorDecodePeer(m); err2 == nil {
			t.mappedAddr = &net.UDPAddr{IP: ip2, Port: port2}
		}
	}

	log.Printf("[TURN-Lite] ✅ Allocation 成功: relay=%s", t.relayAddr)

	safeGo("turn-lite-refresh", t.refreshLoop)
	return nil
}

func (t *TURNLite) ensurePermission(ip net.IP) error {
	key := ip.String()
	t.permMu.Lock()
	last, exists := t.permissions[key]
	t.permMu.Unlock()

	if exists && time.Since(last) < turnPermissionTTL {
		return nil
	}

	peerData := xorEncodePeer(ip, 0)
	resp, err := t.sendRequest(msgCreatePermissionReq, []stunAttr{
		{typ: attrXorPeerAddress, value: peerData},
	}, true)
	if err != nil {
		return fmt.Errorf("CreatePermission: %w", err)
	}
	if resp.msgType != msgCreatePermissionSuc {
		code := parseErrorCode(resp.attrs[attrErrorCode])
		return fmt.Errorf("CreatePermission 被拒 code=%d", code)
	}

	t.permMu.Lock()
	t.permissions[key] = time.Now()
	t.permMu.Unlock()
	return nil
}

func (t *TURNLite) SendTo(data []byte, peerAddr *net.UDPAddr) error {
	if t.conn == nil {
		return fmt.Errorf("TURN 未就绪")
	}
	if len(data) == 0 {
		return nil
	}
	if err := t.ensurePermission(peerAddr.IP); err != nil {
		return err
	}
	peerData := xorEncodePeer(peerAddr.IP, peerAddr.Port)
	msg := buildSTUNMsg(msgSendIndication, randTxID(), []stunAttr{
		{typ: attrXorPeerAddress, value: peerData},
		{typ: attrData, value: data},
	})
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return fmt.Errorf("已关闭")
	}
	_, err := t.conn.Write(msg)
	return err
}

func (t *TURNLite) readLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TURN-Lite] readLoop panic: %v\n%s", r, debug.Stack())
			go t.Close()
		}
	}()
	if t.useTCP {
		t.readLoopTCP()
	} else {
		t.readLoopUDP()
	}
}

func (t *TURNLite) readLoopUDP() {
	buf := make([]byte, 65535)
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		_ = t.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		n, err := t.conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if isNetworkUnreachable(err) {
				log.Printf("[TURN-Lite] 读 socket 失效（网络变化），关闭")
				go t.Close()
				return
			}
			select {
			case <-t.stopCh:
				return
			default:
			}
			log.Printf("[TURN-Lite] UDP 读错误: %v", err)
			return
		}
		t.handleIncoming(buf[:n])
	}
}

func (t *TURNLite) readLoopTCP() {
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		_ = t.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		pkt, err := readTCPPacket(t.conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if isNetworkUnreachable(err) {
				log.Printf("[TURN-Lite] TCP 读 socket 失效（网络变化），关闭")
				go t.Close()
				return
			}
			select {
			case <-t.stopCh:
				return
			default:
			}
			log.Printf("[TURN-Lite] TCP 读错误: %v", err)
			return
		}
		t.handleIncoming(pkt)
	}
}

func (t *TURNLite) handleIncoming(data []byte) {
	if len(data) < 4 {
		return
	}
	if data[0]&0xC0 == 0x40 {
		return
	}
	msg, err := parseSTUNMsg(data)
	if err != nil {
		return
	}
	switch msg.msgType {
	case msgDataIndication:
		peerData := msg.attrs[attrXorPeerAddress]
		dataPayload := msg.attrs[attrData]
		if peerData == nil || dataPayload == nil {
			return
		}
		ip, port, err := xorDecodePeer(peerData)
		if err != nil {
			return
		}
		if t.onMessage != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[TURN-Lite] onMessage panic: %v\n%s", r, debug.Stack())
					}
				}()
				t.onMessage(dataPayload, &net.UDPAddr{IP: ip, Port: port})
			}()
		}
	default:
		t.dispatch(msg)
	}
}

func (t *TURNLite) refreshLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TURN-Lite] refreshLoop panic: %v\n%s", r, debug.Stack())
		}
	}()

	ticker := time.NewTicker(turnRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			if !t.IsAlive() {
				return
			}

			resp, err := t.sendRequest(msgRefreshRequest, []stunAttr{
				{typ: attrLifetime, value: uint32ToBytes(turnDefaultLifetime)},
			}, true)
			if err != nil {
				if isNetworkUnreachable(err) {
					return
				}
				log.Printf("[TURN-Lite] Refresh 失败: %v", err)
				continue
			}
			if resp.msgType == msgRefreshSuccess {
				continue
			}

			code := parseErrorCode(resp.attrs[attrErrorCode])
			if code == 438 {
				newNonce := resp.attrs[attrNonce]
				if len(newNonce) > 0 {
					t.mu.Lock()
					t.nonce = newNonce
					t.mu.Unlock()
					_, err2 := t.sendRequest(msgRefreshRequest, []stunAttr{
						{typ: attrLifetime, value: uint32ToBytes(turnDefaultLifetime)},
					}, true)
					if err2 != nil && !isNetworkUnreachable(err2) {
						log.Printf("[TURN-Lite] 438 重试失败: %v", err2)
					}
				} else {
					log.Printf("[TURN-Lite] 438 但响应无 nonce")
				}
			} else {
				log.Printf("[TURN-Lite] Refresh 被拒: code=%d", code)
			}
		}
	}
}

func (t *TURNLite) GetRelayAddr() string {
	if t.relayAddr == nil {
		return ""
	}
	return t.relayAddr.String()
}

func (t *TURNLite) Close() {
	t.stopOnce.Do(func() {
		t.mu.Lock()
		t.stopped = true
		t.mu.Unlock()
		close(t.stopCh)
	})
	if t.conn != nil {
		_ = t.conn.Close()
	}
}

func uint32ToBytes(v int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func parseErrorCode(data []byte) int {
	if len(data) < 4 {
		return 0
	}
	return int(data[2]&0x07)*100 + int(data[3])
}
