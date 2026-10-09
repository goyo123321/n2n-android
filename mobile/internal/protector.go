package internal

import (
	"fmt"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

var (
	globalProtector   func(fd int) bool
	globalProtectorMu sync.RWMutex
)

// SetProtector 注册 Kotlin 侧 VpnService.protect 桥。传 nil 表示注销。
func SetProtector(fn func(fd int) bool) {
	globalProtectorMu.Lock()
	globalProtector = fn
	globalProtectorMu.Unlock()
}

func currentProtector() func(fd int) bool {
	globalProtectorMu.RLock()
	defer globalProtectorMu.RUnlock()
	return globalProtector
}

func newProtectedDialer() *net.Dialer {
	d := &net.Dialer{Timeout: 10 * time.Second}

	protector := currentProtector()
	if protector == nil {
		return d
	}

	d.Control = func(network, address string, c syscall.RawConn) error {
		var protectErr error
		ctrlErr := c.Control(func(fd uintptr) {
			p := currentProtector()
			if p == nil {
				return
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[protector] panic: %v", r)
						protectErr = fmt.Errorf("protect panic: %v", r)
					}
				}()
				if !p(int(fd)) {
					protectErr = fmt.Errorf("protect fd %d failed", fd)
				}
			}()
		})
		if ctrlErr != nil {
			return ctrlErr
		}
		return protectErr
	}
	return d
}
