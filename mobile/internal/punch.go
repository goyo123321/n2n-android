package internal

// NAT 打洞握手协议 + 数据面探针。
//
// 与 n2n-go-client / n2n-termux 对齐：
//   1. Punch/ACK 使用不同 prefix
//   2. 收到 Punch 回 ACK；收到 ACK 绝不回复（避免 echo loop）
//   3. 数据面探针统一为 N2NS PING 帧（p2p_stream.go 提供）

import (
	"net"
	"sync"
	"time"
)

var (
	punchPrefix = []byte{0x4E, 0x32, 0x4E, 0x50} // "N2NP"
	ackPrefix   = []byte{0x4E, 0x32, 0x4E, 0x41} // "N2NA"
)

// punchAckThrottle 是同一 peer 两次 ACK 的最小间隔。
const punchAckThrottle = 500 * time.Millisecond

func isPunchProbe(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == punchPrefix[0] && data[1] == punchPrefix[1] &&
		data[2] == punchPrefix[2] && data[3] == punchPrefix[3]
}

func isPunchAck(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == ackPrefix[0] && data[1] == ackPrefix[1] &&
		data[2] == ackPrefix[2] && data[3] == ackPrefix[3]
}

// buildPunchProbe 生成 32 字节 Punch。前 4 字节是 punchPrefix，
// 5-8 字节携带本机虚拟 IP（接收方可不解析）。
//
// 全工程唯一实现。nathole_executor.go 不再定义同名函数。
func buildPunchProbe(virtualIP string) []byte {
	buf := make([]byte, 32)
	copy(buf[0:4], punchPrefix)
	if ip := net.ParseIP(virtualIP); ip != nil && ip.To4() != nil {
		copy(buf[4:8], ip.To4())
	}
	return buf
}

// buildPunchAck 生成 8 字节 ACK。
func buildPunchAck() []byte {
	buf := make([]byte, 8)
	copy(buf[0:4], ackPrefix)
	return buf
}

// punchAckLimiter 按 peer 限流 ACK 发送。
//
// 场景：对端在快速重试时可能每秒发多次 Punch。若每次都回 ACK，
// 会成比例放大流量。限流到 500ms/peer。
type punchAckLimiter struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func newPunchAckLimiter() *punchAckLimiter {
	return &punchAckLimiter{last: make(map[string]time.Time)}
}

func (l *punchAckLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.last[key]; ok && now.Sub(t) < punchAckThrottle {
		return false
	}
	l.last[key] = now
	return true
}

func (l *punchAckLimiter) forget(key string) {
	l.mu.Lock()
	delete(l.last, key)
	l.mu.Unlock()
}

func (l *punchAckLimiter) clear() {
	l.mu.Lock()
	l.last = make(map[string]time.Time)
	l.mu.Unlock()
}

// sendP2PProbe 用 N2NS PING 帧触发数据面验证。
//
// 由 P2PStreamManager.sendP2PProbe 实现。这里只是一层包装，
// 便于 edge.go / nathole_executor.go 直接调用 e.sendP2PProbe(peerID)。
func (e *Edge) sendP2PProbe(peerID string) {
	if e.p2pStreams == nil {
		return
	}
	e.p2pStreams.sendP2PProbe(peerID)
}
