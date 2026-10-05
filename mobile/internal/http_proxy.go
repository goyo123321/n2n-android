package internal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type HTTPProxy struct {
	listenAddr string
	listener   net.Listener
	wsOutbound *WSOutbound
	mu         sync.Mutex
	stopped    bool
}

func NewHTTPProxy(listenAddr string, wsOut *WSOutbound) (*HTTPProxy, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("HTTP 代理监听失败: %w", err)
	}
	return &HTTPProxy{
		listenAddr: listenAddr,
		listener:   ln,
		wsOutbound: wsOut,
	}, nil
}

func (p *HTTPProxy) Start() {
	go p.acceptLoop()
	log.Printf("[HTTPProxy] 监听 %s", p.listenAddr)
}

func (p *HTTPProxy) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			p.mu.Lock()
			stopped := p.stopped
			p.mu.Unlock()
			if stopped {
				return
			}
			continue
		}
		go p.handleConn(conn)
	}
}

func (p *HTTPProxy) handleConn(clientConn net.Conn) {
	defer clientConn.Close()
	_ = clientConn.SetReadDeadline(time.Now().Add(30 * time.Second))

	br := bufio.NewReader(clientConn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	if !strings.Contains(host, ":") {
		if req.Method == "CONNECT" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	log.Printf("[HTTPProxy] %s → %s", req.Method, host)

	hostOnly, portStr, _ := net.SplitHostPort(host)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	if port <= 0 {
		clientConn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return
	}

	ipStr := hostOnly
	if net.ParseIP(ipStr) == nil {
		ips, err := net.LookupIP(ipStr)
		if err != nil || len(ips) == 0 {
			clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
			return
		}
		ipStr = ips[0].String()
	}

	if p.wsOutbound == nil {
		clientConn.Write([]byte("HTTP/1.1 503 Service Unavailable\r\n\r\n"))
		return
	}

	remote, err := p.wsOutbound.NewStream(ipStr, port)
	if err != nil {
		log.Printf("[HTTPProxy] 建立 Workers 流失败: %v", err)
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		return
	}
	defer remote.Close()

	_ = clientConn.SetReadDeadline(time.Time{})

	if req.Method == "CONNECT" {
		clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		p.bridge(clientConn, remote)
		return
	}

	req.RequestURI = ""
	req.Header.Del("Proxy-Connection")
	if err := req.Write(remote); err != nil {
		return
	}
	io.Copy(clientConn, remote)
}

func (p *HTTPProxy) bridge(a, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(b, a)
	}()
	go func() {
		defer wg.Done()
		io.Copy(a, b)
	}()
	wg.Wait()
}

func (p *HTTPProxy) Close() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()
	if p.listener != nil {
		p.listener.Close()
	}
}
