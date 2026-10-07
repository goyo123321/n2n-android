package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

const shareServerPort uint16 = 9090

type NetstackHost struct {
	stack     *stack.Stack
	linkEP    *channel.Endpoint
	virtualIP tcpip.Address
	v4        [4]byte
	shareRoot string
	mu        sync.Mutex
	started   bool
	progress  *ProgressDispatcher

	router         *Router
	dnsCache       *DNSCache
	dnsProxy       *DNSProxy
	onProxyTCPConn func(targetIP string, targetPort int) (io.ReadWriteCloser, error)
}

func NewNetstackHost(virtualIP string) (*NetstackHost, error) {
	ip := net.ParseIP(virtualIP).To4()
	if ip == nil {
		return nil, fmt.Errorf("invalid IPv4: %s", virtualIP)
	}
	var v4 [4]byte
	copy(v4[:], ip)

	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	ep := channel.New(1024, 1280, "")
	if err := s.CreateNIC(1, ep); err != nil {
		return nil, fmt.Errorf("CreateNIC: %v", err)
	}

	protoAddr := tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   tcpip.AddrFrom4(v4),
			PrefixLen: 24,
		},
	}
	if err := s.AddProtocolAddress(1, protoAddr, stack.AddressProperties{}); err != nil {
		return nil, fmt.Errorf("AddProtocolAddress: %v", err)
	}

	s.SetRouteTable([]tcpip.Route{{
		Destination: header.IPv4EmptySubnet,
		NIC:         1,
	}})
	s.SetPromiscuousMode(1, true)
	s.SetSpoofing(1, true)

	return &NetstackHost{
		stack:     s,
		linkEP:    ep,
		virtualIP: tcpip.AddrFrom4(v4),
		v4:        v4,
	}, nil
}

// Close 释放 gVisor stack 资源
func (n *NetstackHost) Close() error {
	n.mu.Lock()
	if n.stack == nil {
		n.mu.Unlock()
		return nil
	}
	s := n.stack
	n.stack = nil
	n.started = false
	n.mu.Unlock()

	s.Close()
	s.Wait()
	return nil
}

// ============ 出站集成 ============

func (n *NetstackHost) SetRouter(r *Router, dns *DNSCache) {
	n.router = r
	n.dnsCache = dns
}

func (n *NetstackHost) SetDNSProxy(p *DNSProxy) {
	n.dnsProxy = p
}

func (n *NetstackHost) SetProxyHandler(fn func(ip string, port int) (io.ReadWriteCloser, error)) {
	n.onProxyTCPConn = fn
	n.startTCPForwarder()
	n.startUDPForwarder()
}

// ============ TCP Forwarder ============
//
// gVisor 的 tcp.ForwarderRequest 有 Complete(handshake bool) 方法，
// 所有失败路径必须调用 Complete(true) 释放内部资源，
// 成功路径必须调用 Complete(false) 让 gVisor 建立端点。
func (n *NetstackHost) startTCPForwarder() {
	forwarder := tcp.NewForwarder(n.stack, 0, 65535, func(r *tcp.ForwarderRequest) {
		id := r.ID()
		targetIP := id.LocalAddress.String()
		targetPort := int(id.LocalPort)

		if targetIP == net.IP(n.v4[:]).String() {
			r.Complete(true)
			return
		}

		domain := ""
		if n.dnsCache != nil {
			domain = n.dnsCache.LookupDomainByIP(targetIP)
		}

		var action RouteAction = ActionProxy
		if n.router != nil {
			action, _ = n.router.Match(targetIP, targetPort, domain)
		}

		log.Printf("[Netstack-TCP] %s:%d domain=%q → %s", targetIP, targetPort, domain, action)

		if action == ActionBlock {
			r.Complete(true)
			return
		}

		// ActionDirect：走物理网络直连（protectedDialer 绕过 TUN）
		if action == ActionDirect {
			d := newProtectedDialer()
			rawConn, err := d.Dial("tcp", net.JoinHostPort(targetIP, strconv.Itoa(targetPort)))
			if err != nil {
				log.Printf("[Netstack-TCP] Direct 连接失败: %v", err)
				r.Complete(true)
				return
			}

			var wq waiter.Queue
			ep, epErr := r.CreateEndpoint(&wq)
			if epErr != nil {
				log.Printf("[Netstack-TCP] CreateEndpoint 失败: %v", epErr)
				_ = rawConn.Close()
				r.Complete(true)
				return
			}
			r.Complete(false)

			conn := gonet.NewTCPConn(&wq, ep)
			go func() {
				defer conn.Close()
				defer rawConn.Close()
				go func() { _, _ = io.Copy(rawConn, conn) }()
				_, _ = io.Copy(conn, rawConn)
			}()
			return
		}

		// ActionProxy / ActionP2P：走 Worker / TURN
		if n.onProxyTCPConn == nil {
			r.Complete(true)
			return
		}

		stream, err := n.onProxyTCPConn(targetIP, targetPort)
		if err != nil {
			log.Printf("[Netstack-TCP] Proxy 失败: %v", err)
			r.Complete(true)
			return
		}

		var wq waiter.Queue
		ep, epErr := r.CreateEndpoint(&wq)
		if epErr != nil {
			log.Printf("[Netstack-TCP] CreateEndpoint(proxy) 失败: %v", epErr)
			_ = stream.Close()
			r.Complete(true)
			return
		}
		r.Complete(false)

		conn := gonet.NewTCPConn(&wq, ep)
		go func() {
			defer conn.Close()
			defer stream.Close()
			go func() { _, _ = io.Copy(stream, conn) }()
			_, _ = io.Copy(conn, stream)
		}()
	})

	n.stack.SetTransportProtocolHandler(tcp.ProtocolNumber, forwarder.HandlePacket)
}

// ============ UDP Forwarder ============
//
// gVisor 的 udp.ForwarderRequest 没有 Complete 方法。
// 只能通过 CreateEndpoint 决定是否接受，然后直接 return 即可。
// - 不处理：直接 return，gVisor 会让 sendto 超时或返回错误
// - 处理：CreateEndpoint 成功后，用 goroutine 读写
func (n *NetstackHost) startUDPForwarder() {
	forwarder := udp.NewForwarder(n.stack, func(r *udp.ForwarderRequest) {
		id := r.ID()
		targetIP := id.LocalAddress.String()
		targetPort := int(id.LocalPort)

		log.Printf("[Netstack-UDP] %s:%d", targetIP, targetPort)

		// 只处理 DNS
		if targetPort == 53 {
			n.handleDNSUDP(r)
			return
		}

		// 其他 UDP 直接忽略（gVisor 会自动让 sendto 失败）
	})
	n.stack.SetTransportProtocolHandler(udp.ProtocolNumber, forwarder.HandlePacket)
}

func (n *NetstackHost) handleDNSUDP(r *udp.ForwarderRequest) {
	if n.dnsProxy == nil {
		log.Printf("[Netstack-UDP] DNS proxy 未就绪，忽略 DNS 请求")
		return
	}

	var wq waiter.Queue
	ep, epErr := r.CreateEndpoint(&wq)
	if epErr != nil {
		log.Printf("[Netstack-UDP] CreateEndpoint 失败: %v", epErr)
		return
	}

	conn := gonet.NewUDPConn(&wq, ep)
	go func() {
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			nr, _, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			query := make([]byte, nr)
			copy(query, buf[:nr])

			resp, err := n.dnsProxy.HandleDNSQuery(query)
			if err == nil && resp != nil {
				_, _ = conn.Write(resp)
			}
		}
	}()
}

// ============ TUN 注入/读出 ============

func (n *NetstackHost) InjectTUNPacket(data []byte) {
	if len(data) < 20 {
		return
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(cp),
	})
	n.linkEP.InjectInbound(header.IPv4ProtocolNumber, pkt)
	pkt.DecRef()
}

// ReadTUNPacket 非阻塞读出。没有包时立即返回 (nil, false)。
func (n *NetstackHost) ReadTUNPacket() ([]byte, bool) {
	n.mu.Lock()
	ep := n.linkEP
	closed := n.stack == nil
	n.mu.Unlock()
	if closed || ep == nil {
		return nil, false
	}

	pkt := ep.Read()
	if pkt == nil {
		return nil, false
	}
	defer pkt.DecRef()
	view := pkt.ToView()
	defer view.Release()
	data := view.AsSlice()
	out := make([]byte, len(data))
	copy(out, data)
	return out, true
}

// ReadTUNPacketContext 阻塞读取，直到有包或 ctx 被取消。
func (n *NetstackHost) ReadTUNPacketContext(ctx context.Context) ([]byte, bool) {
	n.mu.Lock()
	ep := n.linkEP
	closed := n.stack == nil
	n.mu.Unlock()
	if closed || ep == nil {
		return nil, false
	}

	for {
		select {
		case <-ctx.Done():
			return nil, false
		default:
		}

		pkt := ep.Read()
		if pkt == nil {
			select {
			case <-ctx.Done():
				return nil, false
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		view := pkt.ToView()
		data := view.AsSlice()
		out := make([]byte, len(data))
		copy(out, data)
		view.Release()
		pkt.DecRef()
		return out, true
	}
}

func (n *NetstackHost) ListenTCP(port uint16) (net.Listener, error) {
	addr := tcpip.FullAddress{
		NIC:  1,
		Addr: n.virtualIP,
		Port: port,
	}
	return gonet.ListenTCP(n.stack, addr, ipv4.ProtocolNumber)
}

// ============ 共享盘 HTTP ============

func (n *NetstackHost) StartShareServer(rootDir string) error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return nil
	}
	n.shareRoot = rootDir
	n.started = true
	n.mu.Unlock()

	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return fmt.Errorf("create share dir: %w", err)
	}

	lis, err := n.ListenTCP(shareServerPort)
	if err != nil {
		return fmt.Errorf("ListenTCP: %w", err)
	}

	mux := n.buildShareMux()
	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  30 * time.Minute,
		WriteTimeout: 30 * time.Minute,
	}

	go func() {
		log.Printf("[Netstack] 共享盘监听 http://%s:%d/", net.IP(n.v4[:]).String(), shareServerPort)
		if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Printf("[Netstack] HTTP 退出: %v", err)
		}
	}()
	return nil
}

func (n *NetstackHost) buildShareMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleIndex)
	mux.HandleFunc("/api/list", n.handleList)
	mux.HandleFunc("/api/download", n.handleDownload)
	mux.HandleFunc("/api/upload", n.handleUpload)
	mux.HandleFunc("/api/delete", n.handleDelete)
	mux.HandleFunc("/api/mkdir", n.handleMkdir)
	mux.HandleFunc("/api/node_info", n.handleNodeInfo)
	return mux
}

func (n *NetstackHost) safePath(rel string) (string, error) {
	clean := filepath.Clean("/" + rel)
	if strings.Contains(clean, "..") {
		return "", fmt.Errorf("invalid path")
	}
	full := filepath.Join(n.shareRoot, clean)
	absRoot, _ := filepath.Abs(n.shareRoot)
	absFull, _ := filepath.Abs(full)
	if !strings.HasPrefix(absFull, absRoot) {
		return "", fmt.Errorf("path escape")
	}
	return full, nil
}

type fileEntry struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"modTime"`
	IsDir   bool   `json:"isDir"`
}

func (n *NetstackHost) handleList(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	full, err := n.safePath(rel)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	entries, err := os.ReadDir(full)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var files []fileEntry
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fileEntry{
			Name: e.Name(), Size: info.Size(),
			ModTime: info.ModTime().UnixMilli(), IsDir: e.IsDir(),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].IsDir != files[j].IsDir {
			return files[i].IsDir
		}
		return files[i].Name < files[j].Name
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(files)
}

func (n *NetstackHost) handleDownload(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	full, err := n.safePath(rel)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	info, err := os.Stat(full)
	if err != nil || info.IsDir() {
		http.Error(w, "not found", 404)
		return
	}
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="%s"`, filepath.Base(full)))
	http.ServeFile(w, r, full)
}

func (n *NetstackHost) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	rel := r.URL.Query().Get("path")
	full, err := n.safePath(rel)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	totalBytes := int(r.ContentLength)
	filename := filepath.Base(full)

	if n.progress != nil {
		n.progress.Start(filename, totalBytes)
	}

	_ = os.MkdirAll(filepath.Dir(full), 0755)
	out, err := os.Create(full)
	if err != nil {
		if n.progress != nil {
			n.progress.Error(filename, err.Error())
		}
		http.Error(w, err.Error(), 500)
		return
	}
	defer out.Close()

	written := int64(0)
	buf := make([]byte, 32*1024)
	lastReport := int64(0)
	reportStep := int64(256 * 1024)

	for {
		nr, er := r.Body.Read(buf)
		if nr > 0 {
			nw, ew := out.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
				if n.progress != nil && written-lastReport >= reportStep {
					n.progress.Progress(filename, int(written), totalBytes)
					lastReport = written
				}
			}
			if ew != nil {
				if n.progress != nil {
					n.progress.Error(filename, ew.Error())
				}
				http.Error(w, ew.Error(), 500)
				return
			}
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			if n.progress != nil {
				n.progress.Error(filename, er.Error())
			}
			http.Error(w, er.Error(), 500)
			return
		}
	}

	if n.progress != nil {
		n.progress.Complete(filename, int(written), true)
	}
	_, _ = fmt.Fprintf(w, "OK: %d bytes", written)
}

func (n *NetstackHost) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "method not allowed", 405)
		return
	}
	rel := r.URL.Query().Get("path")
	full, err := n.safePath(rel)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	_ = os.RemoveAll(full)
	w.WriteHeader(200)
}

func (n *NetstackHost) handleMkdir(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	rel := r.URL.Query().Get("path")
	full, err := n.safePath(rel)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	_ = os.MkdirAll(full, 0755)
	w.WriteHeader(200)
}

func (n *NetstackHost) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"name":      "Android",
		"virtualIp": net.IP(n.v4[:]).String(),
		"port":      int(shareServerPort),
		"webdav":    "",
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(info)
}

func (n *NetstackHost) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(shareIndexHTML))
}

const shareIndexHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Android 共享盘</title>
<style>
body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;padding:16px;margin:0}
h1{font-size:18px;margin-bottom:12px}
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:8px;font-size:14px}
th,td{padding:10px;text-align:left;border-bottom:1px solid #334155}
th{background:#0f172a;color:#94a3b8;font-size:12px}
a{color:#60a5fa;text-decoration:none}
a:hover{text-decoration:underline}
.size{color:#94a3b8;font-family:monospace;font-size:12px}
</style></head><body>
<h1>📁 Android 共享盘</h1>
<table><thead><tr><th>名称</th><th>大小</th><th>时间</th></tr></thead>
<tbody id="list"></tbody></table>
<script>
let cur='';
function fmtSize(b){if(b<1024)return b+' B';if(b<1048576)return(b/1024).toFixed(1)+' KB';if(b<1073741824)return(b/1048576).toFixed(1)+' MB';return(b/1073741824).toFixed(2)+' GB'}
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
async function load(){const r=await fetch('/api/list?path='+encodeURIComponent(cur));const files=await r.json();
const t=document.getElementById('list');
if(!files.length){t.innerHTML='<tr><td colspan="3" style="text-align:center;color:#64748b;padding:24px">空目录</td></tr>';return}
t.innerHTML=files.map(f=>{
const i=f.isDir?'📁':'📄';
const n=f.isDir?'<a onclick="nav(\''+esc(f.name)+'\')">'+i+' '+esc(f.name)+'</a>':'<a href="/api/download?path='+encodeURIComponent(cur+'/'+f.name)+'">'+i+' '+esc(f.name)+'</a>';
return '<tr><td>'+n+'</td><td class="size">'+(f.isDir?'--':fmtSize(f.size))+'</td><td class="size">'+new Date(f.modTime).toLocaleString()+'</td></tr>'
}).join('')}
function nav(n){cur=(cur+'/'+n).replace(/\/+/g,'/');load()}
load();
</script></body></html>`
