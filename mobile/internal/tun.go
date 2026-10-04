package internal

import (
	"os"
)

// TUNDevice 用 fd 包装的 TUN 设备
type TUNDevice struct {
	file *os.File
	mtu  int
}

// setupTUNFromFD 从 Android VpnService.detachFd() 拿到的 fd 包装
func setupTUNFromFD(fd int) (*TUNDevice, error) {
	f := os.NewFile(uintptr(fd), "tun")
	if f == nil {
		return nil, os.ErrInvalid
	}
	return &TUNDevice{file: f, mtu: 1280}, nil
}

func (t *TUNDevice) Read(buf []byte) (int, error) {
	return t.file.Read(buf)
}

func (t *TUNDevice) Write(buf []byte) (int, error) {
	return t.file.Write(buf)
}

func (t *TUNDevice) Close() error {
	return t.file.Close()
}
