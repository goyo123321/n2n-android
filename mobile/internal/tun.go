package internal

import (
	"os"
)

// TUNDevice 用 fd 包装的 TUN 设备。
//
// Android 侧由 VpnService.Builder.establish() 返回 ParcelFileDescriptor，
// detachFd() 后把裸 fd 传给 Go，这里用 os.NewFile 包装。
//
// 直接对 fd 读写，和 PC 端的 /dev/net/tun 用法一致：
//   - Read  返回一个 IPv4 包
//   - Write 写入一个 IPv4 包
type TUNDevice struct {
	file *os.File
	mtu  int
}

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
