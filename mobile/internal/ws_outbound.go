package internal

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
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

const (
	frameOpen     = 0x01
	frameData     = 0x02
	frameClose    = 0x03
	frameOpenFail = 0x04
)

type WSOutbound struct {
	mu      sync.RWMutex
	stopped bool

	conn    *websocket.Conn
	streams map[uint16]*muxStream
	nextID  uint16
	writeMu sync.Mutex

	streamURL     string
	preferredIP   string
	preferredPort int
	sniHost       string

	turnClient *TURNClient
	dialer     *websocket.Dialer

	uuid      string
	clientID  string
	virtualIP string
	udpPort   int
	netInfo   *NetInfo

	connected bool
}

func NewWSOutbound(
	signalingURL, roomId, clientId, connectToken,
	preferredIP string, preferredPort int,
	turnClient *TURNClient,
	uuid, virtualIP string,
	udpPort int,
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
	log.Printf("[WSOut-Mux] 出口端点: %s", streamURL)

	o := &WSOutbound{
		streamURL:     streamURL,
		preferredIP:   preferredIP,
		preferredPort: preferredPort,
		sniHost:       sniHost,
		turnClient:    turnClient,
		streams:       make(map[uint16]*muxStream),
		nextID:        1,
		uuid:          uuid,
		clientID:      clientId,
		virtualIP:     virtualIP,
		udpPort:       udpPort,
	}
	o.dialer = o.buildDialer()
	go o.connectLoop()
	return o, nil
}

func (o *WSOutbound) buildDialer() *websocket.Dialer {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		TLSClientConfig:  &tls.Config{ServerName: o.sniHost},
	}
	protectedDialer := newProtectedDialer()

	var targetAddr string
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
		targetAddr = net.JoinHostPort(preferredHost, strconv.Itoa(preferredPortNum))
	}

	dialer.NetDial = func(network, addr string) (net.Conn, error) {
		actualAddr := targetAddr
		if actualAddr == "" {
			actualAddr = addr
		}
		conn, err := protectedDialer.Dial(network, actualAddr)
		if err == nil {
			return conn, nil
		}
		log.Printf("[WSOut-Mux] 直连 %s 失败: %v", actualAddr, err)

		if o.turnClient != nil && o.turnClient.HasTCPAlloc() {
			host, portStr, splitErr := net.SplitHostPort(actualAddr)
			if splitErr == nil {
				port, _ := strconv.Atoi(portStr)
				if port > 0 {
					stream, turnErr := o.turnClient.DialTCP(host, port)
					if turnErr == nil {
						log.Printf("[WSOut-Mux] ✅ 通过 TURN 中继: %s", actualAddr)
						return newStreamConn(stream, actualAddr), nil
					}
					log.Printf("[WSOut-Mux] TURN 中继失败: %v", turnErr)
				}
			}
		}
		return nil, fmt.Errorf("直连和 TURN 都失败: %v", err)
	}
	return dialer
}

func (o *WSOutbound) connectLoop() {
	for {
		o.mu.RLock()
		stopped := o.stopped
		o.mu.RUnlock()
		if stopped {
			return
		}
		conn, _, err := o.dialer.Dial(o.streamURL, nil)
		if err != nil {
			log.Printf("[WSOut-Mux] 连接失败: %v，5s 后重试", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if err := o.sendAuthFrame(conn); err != nil {
			log.Printf("[WSOut-Mux] 发送认证帧失败: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}
		_, resp, err := conn.ReadMessage()
		if err != nil || len(resp) < 1 || resp[0] != 0x00 {
			log.Printf("[WSOut-Mux] 认证失败: err=%v resp=%v", err, resp)
			conn.Close()
			time.Sleep(30 * time.Second)
			continue
		}
		log.Printf("[WSOut-Mux] ✅ 认证成功，WSS 建立")

		o.mu.Lock()
		o.conn = conn
		o.connected = true
		o.mu.Unlock()

		go o.heartbeat(conn)
		o.readLoop(conn)

		o.mu.Lock()
		o.conn = nil
		o.connected = false
		for id, s := range o.streams {
			s.closeInternal()
			delete(o.streams, id)
		}
		o.mu.Unlock()

		o.mu.RLock()
		stopped = o.stopped
		o.mu.RUnlock()
		if stopped {
			return
		}
		log.Printf("[WSOut-Mux] WSS 断开，1s 后重连")
		time.Sleep(1 * time.Second)
	}
}

func (o *WSOutbound) heartbeat(conn *websocket.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		o.mu.RLock()
		if o.conn != conn {
			o.mu.RUnlock()
			return
		}
		o.mu.RUnlock()
		o.writeMu.Lock()
		err := conn.WriteMessage(websocket.PingMessage, nil)
		o.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (o *WSOutbound) sendAuthFrame(conn *websocket.Conn) error {
	uuidHex := ""
	for _, c := range o.uuid {
		if c != '-' {
			uuidHex += string(c)
		}
	}
	if len(uuidHex) != 32 {
		return fmt.Errorf("UUID 长度不对: %d", len(uuidHex))
	}
	uuidBytes := make([]byte, 16)
	for i := 0; i < 16; i++ {
		var b byte
		fmt.Sscanf(uuidHex[i*2:i*2+2], "%02x", &b)
		uuidBytes[i] = b
	}

	if o.netInfo == nil {
		o.netInfo = getNetInfo()
	}
	meta := map[string]interface{}{
		"clientId":  o.clientID,
		"virtualIp": o.virtualIP,
		"udpPort":   o.udpPort,
		"lanIp":     o.netInfo.LANIP,
		"gatewayIp": o.netInfo.GatewayIP,
	}
	metaJSON, _ := json.Marshal(meta)

	buf := make([]byte, 0, 16+1+2+len(metaJSON))
	buf = append(buf, uuidBytes...)
	buf = append(buf, 0x01)
	buf = append(buf, byte(len(metaJSON)>>8), byte(len(metaJSON)&0xFF))
	buf = append(buf, metaJSON...)

	log.Printf("[WSOut-Mux] 发送认证帧: meta=%s", string(metaJSON))
	return conn.WriteMessage(websocket.BinaryMessage, buf)
}

func (o *WSOutbound) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if len(data) < 5 {
			continue
		}
		id := binary.BigEndian.Uint16(data[0:2])
		typ := data[2]
		length := int(binary.BigEndian.Uint16(data[3:5]))
		if len(data) < 5+length {
			continue
		}
		payload := data[5 : 5+length]

		o.mu.RLock()
		s := o.streams[id]
		o.mu.RUnlock()
		if s == nil {
			continue
		}
		switch typ {
		case frameData:
			cp := make([]byte, len(payload))
			copy(cp, payload)
			select {
			case s.recvCh <- cp:
			default:
			}
		case frameClose, frameOpenFail:
			s.closeInternal()
			o.mu.Lock()
			delete(o.streams, id)
			o.mu.Unlock()
		}
	}
}

func (o *WSOutbound) allocID() uint16 {
	for i := 0; i < 65536; i++ {
		id := o.nextID
		o.nextID++
		if _, used := o.streams[id]; !used {
			return id
		}
	}
	return 0
}

func (o *WSOutbound) sendFrame(typ byte, id uint16, payload []byte) error {
	o.mu.RLock()
	conn := o.conn
	o.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("WSS 未连接")
	}
	buf := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint16(buf[0:2], id)
	buf[2] = typ
	binary.BigEndian.PutUint16(buf[3:5], uint16(len(payload)))
	copy(buf[5:], payload)
	o.writeMu.Lock()
	defer o.writeMu.Unlock()
	return conn.WriteMessage(websocket.BinaryMessage, buf)
}

func (o *WSOutbound) waitConnected(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		o.mu.RLock()
		connected := o.connected
		stopped := o.stopped
		o.mu.RUnlock()
		if connected {
			return true
		}
		if stopped {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (o *WSOutbound) NewStream(targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	o.mu.RLock()
	stopped := o.stopped
	o.mu.RUnlock()
	if stopped {
		return nil, fmt.Errorf("已关闭")
	}

	ip4 := net.ParseIP(targetIP).To4()
	if ip4 == nil {
		return nil, fmt.Errorf("仅支持 IPv4: %s", targetIP)
	}

	// ★ CF CDN IP：Worker 不能连，走 TURN
	if IsCloudflareCDN(targetIP) {
		log.Printf("[WSOut-Mux] 目标 %s 属于 CF CDN，走 TURN", targetIP)
		if o.turnClient != nil && o.turnClient.HasTCPAlloc() {
			turnStream, turnErr := o.turnClient.DialTCP(targetIP, targetPort)
			if turnErr == nil {
				log.Printf("[WSOut-Mux] ✅ TURN 隧道建立 %s:%d", targetIP, targetPort)
				return turnStream, nil
			}
			return nil, fmt.Errorf("CF CDN 目标且 TURN 失败: %v", turnErr)
		}
		return nil, fmt.Errorf("CF CDN 目标 %s 需要 TURN，但 TURN 未就绪", targetIP)
	}

	// 非 CF CDN：优先 Mux
	if o.waitConnected(2 * time.Second) {
		stream, err := o.openMuxStream(ip4, targetIP, targetPort)
		if err == nil {
			return stream, nil
		}
		log.Printf("[WSOut-Mux] openMux 失败: %v", err)
	}

	if o.turnClient != nil && o.turnClient.HasTCPAlloc() {
		log.Printf("[WSOut-Mux] 回退 TURN RFC 6062")
		turnStream, turnErr := o.turnClient.DialTCP(targetIP, targetPort)
		if turnErr == nil {
			return turnStream, nil
		}
		return nil, fmt.Errorf("Mux 和 TURN 都失败: %v", turnErr)
	}
	return nil, fmt.Errorf("Mux 未就绪且无 TURN 回退")
}

func (o *WSOutbound) openMuxStream(ip4 net.IP, targetIP string, targetPort int) (io.ReadWriteCloser, error) {
	o.mu.Lock()
	if !o.connected {
		o.mu.Unlock()
		return nil, fmt.Errorf("WSS 未连接")
	}
	id := o.allocID()
	stream := &muxStream{
		id:         id,
		out:        o,
		recvCh:     make(chan []byte, 128),
		closed:     make(chan struct{}),
		remoteIP:   targetIP,
		remotePort: targetPort,
	}
	o.streams[id] = stream
	o.mu.Unlock()

	payload := make([]byte, 6)
	copy(payload[0:4], ip4)
	binary.BigEndian.PutUint16(payload[4:6], uint16(targetPort))
	if err := o.sendFrame(frameOpen, id, payload); err != nil {
		o.mu.Lock()
		delete(o.streams, id)
		o.mu.Unlock()
		return nil, err
	}
	log.Printf("[WSOut-Mux] 新建 stream id=%d → %s:%d", id, targetIP, targetPort)
	return stream, nil
}

func (o *WSOutbound) Close() {
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return
	}
	o.stopped = true
	conn := o.conn
	o.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

type muxStream struct {
	id         uint16
	out        *WSOutbound
	recvCh     chan []byte
	closed     chan struct{}
	closeOnce  sync.Once
	remoteIP   string
	remotePort int
}

func (s *muxStream) Read(p []byte) (int, error) {
	select {
	case data := <-s.recvCh:
		return copy(p, data), nil
	case <-s.closed:
		select {
		case data := <-s.recvCh:
			return copy(p, data), nil
		default:
			return 0, io.EOF
		}
	}
}

func (s *muxStream) Write(p []byte) (int, error) {
	select {
	case <-s.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	if err := s.out.sendFrame(frameData, s.id, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *muxStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.out.sendFrame(frameClose, s.id, nil)
		s.out.mu.Lock()
		delete(s.out.streams, s.id)
		s.out.mu.Unlock()
	})
	return nil
}

func (s *muxStream) closeInternal() {
	s.closeOnce.Do(func() { close(s.closed) })
}
