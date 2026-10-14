package internal

type Config struct {
	SignalingURL  string
	RoomID        string
	ClientID      string
	NodeName      string
	ConnectToken  string
	PreferredIP   string
	PreferredPort int

	// ★ 新增：SOCKS5 监听地址（留空 = 不启用）
	//
	// 与 VpnService 并行：VpnService 走 TUN，SOCKS5 走 loopback。
	// 默认 "127.0.0.1:1080"，用户可在 UI 上改。
	Socks5Listen string
}
