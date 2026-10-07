package internal

import (
	"fmt"
	"net"
	"syscall"
	"time"
)

var globalProtector func(fd int) bool

func SetProtector(fn func(fd int) bool) {
	globalProtector = fn
}

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
