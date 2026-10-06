module github.com/goyo123321a/n2n-android/mobile

go 1.26.3

require (
	github.com/gorilla/websocket v1.5.1
	github.com/pion/stun/v2 v2.0.0
	// 升级到兼容 Go 1.26 的版本
	gvisor.dev/gvisor v0.0.0-20260617234043-9307ff8f53c4
)

require (
	github.com/google/btree v1.1.2 // indirect
	github.com/pion/dtls/v2 v2.2.7 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/transport/v2 v2.2.1 // indirect
	github.com/pion/transport/v3 v3.0.7 // indirect
	github.com/wlynxg/anet v0.0.3 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/exp v0.0.0-20250711185948-6ae5c78190dc // indirect
	golang.org/x/mobile v0.0.0-20260908204917-8b95e45f8d3e // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.50.0 // indirect
)

tool (
	golang.org/x/mobile/cmd/gobind
	golang.org/x/mobile/cmd/gomobile
)
