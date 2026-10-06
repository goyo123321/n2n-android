package internal

import (
	"fmt"
	"net"
	"syscall"
	"time"
)

var globalProtector func(fd int) bool

// SetProtector 由 mobile.go 调用，注册 socket 保护函数
func SetProtector(fn func(fd int) bool) {
	globalProtector = fn
}

// newProtectedDialer 返回带 Control 回调的 dialer
// Control 在 socket 创建后、连接前调用，用于 protect(fd) 绕过 VPN
func newProtectedDialer() *net.Dialer {
	d := &net.Dialer{Timeout: 10 * time.Second}
	if globalProtector != nil {
		d.Control = func(network, address string, c syscall.RawConn) error {
			var protectErr error
			c.Control(func(fd uintptr) {
				if !globalProtector(int(fd)) {
					protectErr = fmt.Errorf("protect fd %d failed", fd)
				}
			})
			return protectErr
		}
	}
	return d
}
