package internal

// SOCKS5 入口。与 VpnService 并行运行：
//   - VpnService：系统级 TUN，应用透明
//   - SOCKS5    ：仅在用户显式配置走代理的应用中生效
//
// 用途：不想让所有 App 流量走 VPN 时，可以关掉 VPN 的某些应用，
// 只让需要的 App 通过 127.0.0.1:1080 访问组网内的服务。
//
// ★ 修复：拆成 Listen() / Serve() 两步。
//   Listen() 同步监听，失败返回 error（供 internal.Start 判断端口冲突）。
//   Serve() 阻塞 Accept 循环，前提是 Listen() 已成功。
//   旧版 Serve() 里静默 return，上层不知道端口冲突，用户看到"启动成功"但 SOCKS5 没起来。

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	socks5Version                 = 0x05
	socks5NoAuth                  = 0x00
	socks5CmdConnect              = 0x01
	socks5AtypIPv4                = 0x01
	socks5AtypDomain              = 0x03
	socks5AtypIPv6                = 0x04
	socks5RepSuccess              = 0x00
	socks5RepGeneralFailure       = 0x01
	socks5RepHostUnreachable      = 0x04
	socks5RepCommandNotSupported  = 0x07
	socks5RepAddrTypeNotSupported = 0x08
)

type Socks5Server struct {
	edge     *Edge
	listen   string
	ln       net.Listener
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewSocks5Server(edge *Edge, listen string) *Socks5Server {
	return &Socks5Server{
		edge:   edge,
		listen: listen,
		stopCh: make(chan struct{}),
	}
}

// Listen 同步监听。失败返回 error。
func (s *Socks5Server) Listen() error {
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return fmt.Errorf("SOCKS5 监听 %s 失败: %w", s.listen, err)
	}
	s.ln = ln
	log.Printf("[SOCKS5] 监听 %s（用 curl --socks5 或 ALL_PROXY 访问虚拟 IP）", s.listen)
	return nil
}

// Serve 阻塞 Accept 循环，前提是 Listen() 已成功。
func (s *Socks5Server) Serve() {
	if s.ln == nil {
		log.Printf("[SOCKS5] Serve 调用时 ln 为 nil（Listen 未调用？），退出")
		return
	}
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
			}
			log.Printf("[SOCKS5] Accept 失败: %v", err)
			return
		}
		safeGo("socks5-conn", func() { s.handleConn(conn) })
	}
}

func (s *Socks5Server) Close() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

func (s *Socks5Server) handleConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[SOCKS5] handleConn panic: %v", r)
		}
	}()

	buf := make([]byte, 256)

	if _, err := io.ReadFull(conn, buf[:2]); err != nil {
		_ = conn.Close()
		return
	}
	if buf[0] != socks5Version {
		_ = conn.Close()
		return
	}
	nMethods := int(buf[1])
	if nMethods < 1 || nMethods > 255 {
		_ = conn.Close()
		return
	}
	if _, err := io.ReadFull(conn, buf[:nMethods]); err != nil {
		_ = conn.Close()
		return
	}
	if _, err := conn.Write([]byte{socks5Version, socks5NoAuth}); err != nil {
		_ = conn.Close()
		return
	}

	if _, err := io.ReadFull(conn, buf[:4]); err != nil {
		_ = conn.Close()
		return
	}
	if buf[0] != socks5Version {
		_ = conn.Close()
		return
	}
	if buf[1] != socks5CmdConnect {
		writeSocks5Reply(conn, socks5RepCommandNotSupported)
		_ = conn.Close()
		return
	}

	var dstIP string
	var dstPort uint16

	switch buf[3] {
	case socks5AtypIPv4:
		if _, err := io.ReadFull(conn, buf[:6]); err != nil {
			_ = conn.Close()
			return
		}
		dstIP = fmt.Sprintf("%d.%d.%d.%d", buf[0], buf[1], buf[2], buf[3])
		dstPort = binary.BigEndian.Uint16(buf[4:6])
	case socks5AtypDomain:
		if _, err := io.ReadFull(conn, buf[:1]); err != nil {
			_ = conn.Close()
			return
		}
		nameLen := int(buf[0])
		if nameLen == 0 || nameLen > 253 {
			_ = conn.Close()
			return
		}
		if _, err := io.ReadFull(conn, buf[:nameLen+2]); err != nil {
			_ = conn.Close()
			return
		}
		log.Printf("[SOCKS5] 不支持域名 %s（请用虚拟 IP）", string(buf[:nameLen]))
		writeSocks5Reply(conn, socks5RepAddrTypeNotSupported)
		_ = conn.Close()
		return
	default:
		writeSocks5Reply(conn, socks5RepAddrTypeNotSupported)
		_ = conn.Close()
		return
	}

	log.Printf("[SOCKS5] CONNECT %s:%d", dstIP, dstPort)

	peerID, ok := s.edge.findPeerByVIP(dstIP)
	if !ok {
		log.Printf("[SOCKS5] 找不到 VIP %s 对应的 peer", dstIP)
		writeSocks5Reply(conn, socks5RepHostUnreachable)
		_ = conn.Close()
		return
	}

	// 优先走 P2P / LAN / P2PAvailable / TURN
	useP2P := s.edge.p2pStreams != nil &&
		s.edge.relayMgr != nil &&
		(s.edge.relayMgr.IsP2P(peerID) ||
			s.edge.relayMgr.IsLAN(peerID) ||
			s.edge.relayMgr.IsP2PAvailable(peerID) ||
			s.edge.relayMgr.IsTURN(peerID))

	if useP2P {
		p2s, err := s.edge.p2pStreams.OpenP2PStream(peerID, int(dstPort), conn, 2*time.Second)
		if err == nil {
			log.Printf("[SOCKS5] CONNECT %s:%d → P2P 直连 (stream=%d)", dstIP, dstPort, p2s.ID)
			if werr := writeSocks5Reply(conn, socks5RepSuccess); werr != nil {
				p2s.close("reply-fail")
				_ = conn.Close()
				return
			}
			return
		}
		log.Printf("[SOCKS5] P2P 建流失败(%v)，回落 WS 中继", err)
	}

	stream, err := s.edge.openLocalStream(peerID, int(dstPort), conn)
	if err != nil {
		log.Printf("[SOCKS5] 打开 stream 失败: %v", err)
		writeSocks5Reply(conn, socks5RepGeneralFailure)
		_ = conn.Close()
		return
	}

	if !stream.WaitAck(10 * time.Second) {
		log.Printf("[SOCKS5] stream %s 未收到 ack", stream.ID)
		writeSocks5Reply(conn, socks5RepHostUnreachable)
		s.edge.closeStream(stream, "ack-timeout")
		_ = conn.Close()
		return
	}

	if err := writeSocks5Reply(conn, socks5RepSuccess); err != nil {
		s.edge.closeStream(stream, "reply-fail")
		_ = conn.Close()
		return
	}

	log.Printf("[SOCKS5] CONNECT %s:%d → WS 中继 (stream=%s)", dstIP, dstPort, stream.ID)
	safeGo("socks5-pump", func() { s.edge.pumpLocalToRemote(stream) })
}

func writeSocks5Reply(conn net.Conn, rep byte) error {
	_, err := conn.Write([]byte{
		socks5Version, rep, 0, socks5AtypIPv4,
		0, 0, 0, 0, 0, 0,
	})
	return err
}
