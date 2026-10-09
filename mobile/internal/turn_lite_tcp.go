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
	"sync"
	"time"
)

// RFC 6062 §7
const (
	msgConnectRequest    = 0x000A
	msgConnectSuccess    = 0x010A
	msgConnectError      = 0x011A
	msgConnectionBindReq = 0x000B
	msgConnectionBindSuc = 0x010B
	msgConnectionBindErr = 0x011B
)

const attrConnectionID = 0x002A
const transportTCP = 6

type TURNTCPAllocation struct {
	serverAddr string
	relayHost  string

	username string
	password string

	realm string
	nonce []byte
	key   []byte

	controlConn net.Conn

	pendingMu sync.Mutex
	pending   map[[12]byte]chan *stunMessage

	relayAddr  *net.UDPAddr
	mappedAddr *net.UDPAddr

	mu      sync.Mutex
	stopped bool
	stopCh  chan struct{}
}

func NewTURNTCPAllocation(serverAddr, username, password string) *TURNTCPAllocation {
	return &TURNTCPAllocation{
		serverAddr: serverAddr,
		username:   username,
		password:   password,
		pending:    make(map[[12]byte]chan *stunMessage),
		stopCh:     make(chan struct{}),
	}
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

	ch := make(chan *stunMessage, 1)
	t.pendingMu.Lock()
	t.pending[txid] = ch
	t.pendingMu.Unlock()
	defer func() {
		t.pendingMu.Lock()
		delete(t.pending, txid)
		t.pendingMu.Unlock()
	}()

	if _, err := t.controlConn.Write(msg); err != nil {
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

func (t *TURNTCPAllocation) dispatch(msg *stunMessage) {
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

func (t *TURNTCPAllocation) readControlLoop() {
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
		t.dispatch(msg)
	}
}

func (t *TURNTCPAllocation) Allocate() error {
	dialer := newProtectedDialer()
	conn, err := dialer.Dial("tcp", t.serverAddr)
	if err != nil {
		return fmt.Errorf("连接 TURN control 失败: %w", err)
	}
	t.controlConn = conn
	go t.readControlLoop()

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
	log.Printf("[TURN-TCP] ✅ TCP Allocation 成功: relay=%s", t.relayAddr)
	go t.refreshLoop()
	return nil
}

func (t *TURNTCPAllocation) Dial(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
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
		return nil, fmt.Errorf("Connect 被拒: code=%d (type=0x%04x)", code, connectResp.msgType)
	}

	connectionID := connectResp.attrs[attrConnectionID]
	if len(connectionID) < 1 {
		return nil, fmt.Errorf("Connect 响应缺 ConnectionID")
	}

	dataAddr := net.JoinHostPort(t.relayHost, fmt.Sprintf("%d", t.relayAddr.Port))
	log.Printf("[TURN-TCP] 建立 data connection → %s", dataAddr)

	dialer := newProtectedDialer()
	dataConn, err := dialer.Dial("tcp", dataAddr)
	if err != nil {
		return nil, fmt.Errorf("data connection 失败: %w", err)
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
		_ = dataConn.Close()
		return nil, fmt.Errorf("发送 ConnectionBind 失败: %w", err)
	}

	_ = dataConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	respPkt, err := readTCPPacket(dataConn)
	if err != nil {
		_ = dataConn.Close()
		return nil, fmt.Errorf("读 ConnectionBind 响应失败: %w", err)
	}
	bindResp, err := parseSTUNMsg(respPkt)
	if err != nil {
		_ = dataConn.Close()
		return nil, fmt.Errorf("解析 ConnectionBind 响应失败: %w", err)
	}
	if bindResp.msgType != msgConnectionBindSuc {
		code := parseErrorCode(bindResp.attrs[attrErrorCode])
		_ = dataConn.Close()
		return nil, fmt.Errorf("ConnectionBind 被拒: code=%d (type=0x%04x)", code, bindResp.msgType)
	}

	_ = dataConn.SetReadDeadline(time.Time{})
	log.Printf("[TURN-TCP] ✅ 隧道建立 → %s:%d (connID=%x)", targetIP, targetPort, connectionID)
	return dataConn, nil
}

// refreshLoop 每 60 秒刷新 RFC 6062 TCP allocation。
//
// ★ 438 Stale Nonce 是 TURN 服务器的正常行为，静默重试，不打日志。
func (t *TURNTCPAllocation) refreshLoop() {
	ticker := time.NewTicker(turnRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			resp, err := t.sendRequest(msgRefreshRequest, []stunAttr{
				{typ: attrLifetime, value: uint32ToBytes(turnDefaultLifetime)},
			}, true)
			if err != nil {
				log.Printf("[TURN-TCP] Refresh 失败: %v", err)
				continue
			}
			if resp.msgType == msgRefreshSuccess {
				continue
			}

			code := parseErrorCode(resp.attrs[attrErrorCode])
			if code == 438 {
				// ★ 静默重试
				newNonce := resp.attrs[attrNonce]
				if len(newNonce) > 0 {
					t.mu.Lock()
					t.nonce = newNonce
					t.mu.Unlock()
					_, err2 := t.sendRequest(msgRefreshRequest, []stunAttr{
						{typ: attrLifetime, value: uint32ToBytes(turnDefaultLifetime)},
					}, true)
					if err2 != nil {
						log.Printf("[TURN-TCP] 438 重试失败: %v", err2)
					}
				} else {
					log.Printf("[TURN-TCP] 438 但响应无 nonce")
				}
			} else {
				log.Printf("[TURN-TCP] Refresh 被拒: code=%d", code)
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
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	close(t.stopCh)
	t.mu.Unlock()
	if t.controlConn != nil {
		_ = t.controlConn.Close()
	}
}
