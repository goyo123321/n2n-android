package internal

import (
	"io"
	"net"
	"time"
)

// streamConn 把 io.ReadWriteCloser 包装成 net.Conn
// 用于让 http.Transport 通过 wsOutbound / TURN 建立 HTTPS 连接
type streamConn struct {
	rwc    io.ReadWriteCloser
	remote string
	local  string
}

func newStreamConn(rwc io.ReadWriteCloser, remote string) net.Conn {
	return &streamConn{
		rwc:    rwc,
		remote: remote,
		local:  "10.64.0.1:0",
	}
}

func (c *streamConn) Read(b []byte) (int, error)  { return c.rwc.Read(b) }
func (c *streamConn) Write(b []byte) (int, error) { return c.rwc.Write(b) }
func (c *streamConn) Close() error                { return c.rwc.Close() }

func (c *streamConn) LocalAddr() net.Addr                { return dummyAddr(c.local) }
func (c *streamConn) RemoteAddr() net.Addr               { return dummyAddr(c.remote) }
func (c *streamConn) SetDeadline(t time.Time) error      { return nil }
func (c *streamConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *streamConn) SetWriteDeadline(t time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return "tcp" }
func (a dummyAddr) String() string  { return string(a) }
