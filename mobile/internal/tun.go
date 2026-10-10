package internal

import "os"

// TUNDevice 用 fd 包装的 TUN 设备。
//
// Android 侧由 VpnService.Builder.establish() 返回 ParcelFileDescriptor，
// detachFd() 后把裸 fd 传给 Go，这里用 os.NewFile 包装。
//
// MTU 由 Kotlin 侧的 VpnService.Builder.setMtu() 控制，Go 侧无需感知。
type TUNDevice struct {
	file *os.File
}

func setupTUNFromFD(fd int) (*TUNDevice, error) {
	f := os.NewFile(uintptr(fd), "tun")
	if f == nil {
		return nil, os.ErrInvalid
	}
	return &TUNDevice{file: f}, nil
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
