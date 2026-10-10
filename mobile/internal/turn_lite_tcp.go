package internal

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// ============ RFC 6062 常量 ============

const (
	msgConnectRequest    = 0x000A
	msgConnectSuccess    = 0x000B
	msgConnectError      = 0x000C
	msgConnectionBindReq = 0x000D
	msgConnectionBindSuc = 0x000E
	msgConnectionBindErr = 0x000F
)

const attrConnectionID = 0x002A
const transportTCP = 6

// ============ TCP 分帧读取 ============

// readTCPPacket 从 TCP 连接读一个 STUN 包。
//
// RFC 5766 §7.2.2：TURN over TCP 分帧时，每条消息前 2 bit 是 0b01，
// 消息长度按 4 字节对齐（padding 不计入 length 字段）。
func readTCPPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	if header[0]&0xC0 == 0x40 {
		length := int(binary.BigEndian.Uint16(header[2:4]))
		totalAfterHeader := length + ((4 - length%4) & 3)
		body := make([]byte, totalAfterHeader)
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
		full := make([]byte, 4+length)
		copy(full, header)
		copy(full[4:], body[:length])
		return full, nil
	}

	rest := make([]byte, 16)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return nil, err
	}
	bodyLen := int(binary.BigEndian.Uint16(header[2:4]))
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
	}
	full := make([]byte, 20+bodyLen)
	copy(full, header)
	copy(full[4:], rest)
	copy(full[20:], body)
	return full, nil
}

// ============ tunnel 缓存 ============

type tcpTunnel struct {
	conn net.Conn
	peer string
}

// ============ TURNTCPAllocation ============

type TURNTCPAllocation struct {
	serverAddr string
	relayHost  string

	username string
	password string

	realm string
	nonce []byte
	key   []byte

	controlConn net.Conn
	respCh      chan *stunMessage

	relayAddr  *net.UDPAddr
	mappedAddr *net.UDPAddr

	tunnels   map[string]*tcpTunnel
	tunnelsMu sync.Mutex

	onMessage func(data []byte, addr net.Addr)

	mu        sync.Mutex
	stopped   bool
	stopCh    chan struct{}
	stopOnce  sync.Once
	alive     bool
	failCount int
}

func NewTURNTCPAllocation(serverAddr, username, password string) *TURNTCPAllocation {
	return &TURNTCPAllocation{
		serverAddr: serverAddr,
		username:   username,
		password:   password,
		respCh:     make(chan *stunMessage, 16),
		tunnels:    make(map[string]*tcpTunnel),
		stopCh:     make(chan struct{}),
	}
}

func (t *TURNTCPAllocation) IsAlive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.alive && !t.stopped
}

func (t *TURNTCPAllocation) sign(msg []byte) []byte {
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

func (t *TURNTCPAllocation) sendRequest(msgType uint16, attrs []stunAttr, withAuth bool) (*stunMessage, error) {
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

	for {
		select {
		case <-t.respCh:
			continue
		default:
		}
		break
	}

	if _, err := t.controlConn.Write(msg); err != nil {
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	timeout := time.After(turnRequestTimeout)
	for {
		select {
		case resp := <-t.respCh:
			if resp == nil || resp.txid != txid {
				continue
			}
			return resp, nil
		case <-timeout:
			return nil, fmt.Errorf("请求超时 (type=0x%04x)", msgType)
		case <-t.stopCh:
			return nil, fmt.Errorf("已关闭")
		}
	}
}

func (t *TURNTCPAllocation) readControlLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TURN-TCP] readControlLoop panic: %v\n%s", r, debug.Stack())
		}
	}()

	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		_ = t.controlConn.SetReadDeadline(time.Now().Add(60 * time.Second))
		pkt, err := readTCPPacket(t.controlConn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			select {
			case <-t.stopCh:
				return
			default:
			}
			log.Printf("[TURN-TCP] control 读错误: %v", err)
			return
		}
		msg, err := parseSTUNMsg(pkt)
		if err != nil {
			continue
		}
		select {
		case t.respCh <- msg:
		default:
		}
	}
}

func (t *TURNTCPAllocation) Allocate() error {
	dialer := newProtectedDialer()
	conn, err := dialer.Dial("tcp", t.serverAddr)
	if err != nil {
		return fmt.Errorf("连接 TURN control 失败: %w", err)
	}
	t.controlConn = conn

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	safeGo("turn-tcp-control-read", t.readControlLoop)

	reqTransport := []byte{transportTCP, 0, 0, 0}

	log.Printf("[TURN-TCP] 发送初始 Allocate（TCP, 无认证）")
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
		log.Printf("[TURN-TCP] 401 realm=%q", realm)
	} else if resp.msgType == msgAllocateSuccess {
		return t.extractAllocation(resp)
	} else {
		t.Close()
		return fmt.Errorf("初始 Allocate 返回意外类型 0x%04x", resp.msgType)
	}

	log.Printf("[TURN-TCP] 发送 Allocate（TCP, 带认证）")
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

	return t.extractAllocation(resp)
}

func (t *TURNTCPAllocation) extractAllocation(resp *stunMessage) error {
	relayData := resp.attrs[attrXorRelayedAddr]
	ip, port, err := xorDecodePeer(relayData)
	if err != nil {
		return fmt.Errorf("解析 relay 失败: %w", err)
	}
	t.relayAddr = &net.UDPAddr{IP: ip, Port: port}
	t.relayHost = ip.String()

	if m := resp.attrs[attrXorMappedAddress]; m != nil {
		if ip2, port2, err2 := xorDecodePeer(m); err2 == nil {
			t.mappedAddr = &net.UDPAddr{IP: ip2, Port: port2}
		}
	}

	t.mu.Lock()
	t.alive = true
	t.mu.Unlock()

	log.Printf("[TURN-TCP] ✅ TCP Allocation 成功: relay=%s", t.relayAddr)

	safeGo("turn-tcp-refresh", t.refreshLoop)
	return nil
}

// Dial 建立到目标 peer 的 TCP 隧道（RFC 6062）。
func (t *TURNTCPAllocation) Dial(targetIP string, targetPort int) (net.Conn, error) {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return nil, fmt.Errorf("已关闭")
	}
	t.mu.Unlock()

	ip := net.ParseIP(targetIP)
	if ip == nil {
		return nil, fmt.Errorf("目标 IP 非法: %s", targetIP)
	}

	peerData := xorEncodePeer(ip, targetPort)
	log.Printf("[TURN-TCP] Connect → %s:%d", targetIP, targetPort)
	connectResp, err := t.sendRequest(msgConnectRequest, []stunAttr{
		{typ: attrXorPeerAddress, value: peerData},
	}, true)
	if err != nil {
		return nil, fmt.Errorf("Connect 请求失败: %w", err)
	}
	if connectResp.msgType != msgConnectSuccess {
		code := parseErrorCode(connectResp.attrs[attrErrorCode])
		return nil, fmt.Errorf("Connect 被拒: code=%d", code)
	}

	connectionID := connectResp.attrs[attrConnectionID]
	if len(connectionID) < 4 {
		return nil, fmt.Errorf("Connect 响应缺 ConnectionID")
	}

	dataAddr := net.JoinHostPort(t.relayHost, fmt.Sprintf("%d", t.relayAddr.Port))
	log.Printf("[TURN-TCP] 建立 data connection → %s", dataAddr)

	dialer := newProtectedDialer()
	dataConn, err := dialer.Dial("tcp", dataAddr)
	if err != nil {
		return nil, fmt.Errorf("data connection 失败: %w", err)
	}

	if tcpConn, ok := dataConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	txid := randTxID()
	bindAttrs := []stunAttr{
		{typ: attrConnectionID, value: connectionID},
		{typ: attrUsername, value: []byte(t.username)},
		{typ: attrRealm, value: []byte(t.realm)},
		{typ: attrNonce, value: t.nonce},
	}
	bindMsg := buildSTUNMsg(msgConnectionBindReq, txid, bindAttrs)
	bindMsg = t.sign(bindMsg)

	if _, err := dataConn.Write(bindMsg); err != nil {
		dataConn.Close()
		return nil, fmt.Errorf("发送 ConnectionBind 失败: %w", err)
	}

	_ = dataConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	respPkt, err := readTCPPacket(dataConn)
	if err != nil {
		dataConn.Close()
		return nil, fmt.Errorf("读 ConnectionBind 响应失败: %w", err)
	}
	bindResp, err := parseSTUNMsg(respPkt)
	if err != nil {
		dataConn.Close()
		return nil, fmt.Errorf("解析 ConnectionBind 响应失败: %w", err)
	}
	if bindResp.msgType != msgConnectionBindSuc {
		code := parseErrorCode(bindResp.attrs[attrErrorCode])
		dataConn.Close()
		return nil, fmt.Errorf("ConnectionBind 被拒: code=%d", code)
	}

	_ = dataConn.SetReadDeadline(time.Time{})
	log.Printf("[TURN-TCP] ✅ 隧道建立 → %s:%d (connID=%x)", targetIP, targetPort, connectionID)
	return dataConn, nil
}

// SendTo 给指定 peer 发送数据。首次会建立 tunnel。
func (t *TURNTCPAllocation) SendTo(peerAddr *net.UDPAddr, data []byte) error {
	if !t.IsAlive() {
		return fmt.Errorf("TCP allocation 未就绪")
	}
	if peerAddr == nil {
		return fmt.Errorf("peerAddr 为 nil")
	}
	key := peerAddr.String()

	t.tunnelsMu.Lock()
	tun, ok := t.tunnels[key]
	t.tunnelsMu.Unlock()

	if !ok {
		conn, err := t.Dial(peerAddr.IP.String(), peerAddr.Port)
		if err != nil {
			return fmt.Errorf("建立 tunnel 失败: %w", err)
		}
		tun = &tcpTunnel{conn: conn, peer: key}
		t.tunnelsMu.Lock()
		t.tunnels[key] = tun
		t.tunnelsMu.Unlock()
		safeGo("turn-tcp-tunnel-read", func() {
			t.tunnelReadLoop(tun, peerAddr)
		})
	}

	_, err := tun.conn.Write(data)
	return err
}

func (t *TURNTCPAllocation) tunnelReadLoop(tun *tcpTunnel, peerAddr *net.UDPAddr) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TURN-TCP] tunnel readLoop panic: %v\n%s", r, debug.Stack())
		}
		_ = tun.conn.Close()
		t.tunnelsMu.Lock()
		if cur, ok := t.tunnels[tun.peer]; ok && cur == tun {
			delete(t.tunnels, tun.peer)
		}
		t.tunnelsMu.Unlock()
	}()

	buf := make([]byte, 65535)
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		n, err := tun.conn.Read(buf)
		if err != nil {
			return
		}
		if n > 0 && t.onMessage != nil {
			data := make([]byte, n)
			copy(data, buf[:n])
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[TURN-TCP] onMessage panic: %v\n%s", r, debug.Stack())
					}
				}()
				t.onMessage(data, peerAddr)
			}()
		}
	}
}

func (t *TURNTCPAllocation) refreshLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TURN-TCP] refreshLoop panic: %v\n%s", r, debug.Stack())
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
			_, err := t.sendRequest(msgRefreshRequest, []stunAttr{
				{typ: attrLifetime, value: uint32ToBytes(turnDefaultLifetime)},
			}, true)
			if err != nil {
				t.mu.Lock()
				t.failCount++
				fc := t.failCount
				t.mu.Unlock()
				log.Printf("[TURN-TCP] Refresh 失败 (%d/3): %v", fc, err)
				if fc >= 3 {
					log.Printf("[TURN-TCP] Refresh 连续失败 %d 次，关闭等待重建", fc)
					t.Close()
					return
				}
			} else {
				t.mu.Lock()
				t.failCount = 0
				t.mu.Unlock()
			}
		}
	}
}

func (t *TURNTCPAllocation) GetRelayAddr() string {
	if t.relayAddr == nil {
		return ""
	}
	return t.relayAddr.String()
}

func (t *TURNTCPAllocation) Close() {
	needClose := false
	t.stopOnce.Do(func() {
		t.mu.Lock()
		t.stopped = true
		t.alive = false
		t.mu.Unlock()
		close(t.stopCh)
		needClose = true
	})
	if !needClose {
		return
	}

	if t.controlConn != nil {
		_ = t.controlConn.Close()
	}

	t.tunnelsMu.Lock()
	for _, tun := range t.tunnels {
		_ = tun.conn.Close()
	}
	t.tunnels = make(map[string]*tcpTunnel)
	t.tunnelsMu.Unlock()
}
