#!/usr/bin/env bash
# 删除 gomobile 不支持的 ProgressListener 相关代码
set -e

cd "$(dirname "$0")/.."

echo "=== 1. 重写 mobile/mobile.go ==="
cat > mobile/mobile.go <<'GOEOF'
// Package mobile 是 gomobile 的桥接层。
package mobile

import (
	"log"
	"sync"
	"time"

	"github.com/goyo123321a/n2n-android/mobile/internal"
)

type Config struct {
	SignalingURL string
	RoomID       string
	ClientID     string
	NodeName     string
	ConnectToken string
	ShareDir     string
}

type Client struct {
	mu        sync.Mutex
	tunFd     int
	running   bool
	virtualIP string
	clientID  string
	edge      *internal.Edge
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) SetTunFD(fd int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tunFd = fd
}

func (c *Client) Start(cfg *Config) string {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return "already running"
	}
	if c.tunFd <= 0 {
		c.mu.Unlock()
		return "tun fd not set"
	}
	tunFd := c.tunFd
	c.mu.Unlock()

	icfg := &internal.Config{
		SignalingURL: cfg.SignalingURL,
		RoomID:       cfg.RoomID,
		ClientID:     cfg.ClientID,
		NodeName:     cfg.NodeName,
		ConnectToken: cfg.ConnectToken,
		ShareDir:     cfg.ShareDir,
	}

	edge, err := internal.Start(icfg, tunFd)
	if err != nil {
		return err.Error()
	}

	c.mu.Lock()
	c.edge = edge
	c.running = true
	if cfg.ClientID != "" {
		c.clientID = cfg.ClientID
	} else {
		c.clientID = edge.GetClientID()
	}
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			c.mu.Lock()
			running := c.running
			edgeRef := c.edge
			c.mu.Unlock()
			if !running || edgeRef == nil {
				return
			}
			if vip := edgeRef.GetVirtualIP(); vip != "" {
				c.mu.Lock()
				c.virtualIP = vip
				c.mu.Unlock()
			}
			select {
			case <-edgeRef.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	log.Printf("[mobile] started, room=%s", cfg.RoomID)
	return ""
}

func (c *Client) Stop() {
	c.mu.Lock()
	edge := c.edge
	c.edge = nil
	c.running = false
	c.virtualIP = ""
	c.mu.Unlock()

	if edge != nil {
		edge.Stop()
	}
	log.Printf("[mobile] stopped")
}

func (c *Client) GetStatus() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return "stopped"
	}
	if c.virtualIP == "" {
		return "starting"
	}
	return "running"
}

func (c *Client) GetVirtualIP() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.virtualIP
}

func (c *Client) GetClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

func (c *Client) GetPeersJSON() string {
	c.mu.Lock()
	edge := c.edge
	c.mu.Unlock()
	if edge == nil {
		return "[]"
	}
	return edge.GetPeersJSON()
}
GOEOF
echo "✅ mobile/mobile.go 已重写"

echo ""
echo "=== 2. 删除 edge.go 里的 SetProgressListener 方法 ==="
python3 <<'PYEOF'
path = "mobile/internal/edge.go"
with open(path) as f:
    lines = f.read().split('\n')

out = []
i = 0
while i < len(lines):
    line = lines[i]
    if 'func (e *Edge) SetProgressListener' in line:
        # 跳过整个方法
        depth = 0
        while i < len(lines):
            depth += lines[i].count('{') - lines[i].count('}')
            i += 1
            if depth <= 0 and '}' in lines[i-1]:
                break
        # 也删掉前面的注释行
        while out and out[-1].strip().startswith('//'):
            out.pop()
        continue
    out.append(line)
    i += 1

with open(path, 'w') as f:
    f.write('\n'.join(out))

print("✅ edge.go 处理完成")
PYEOF

echo ""
echo "=== 3. 重写 N2nController.kt ==="
cat > app/src/main/java/com/n2n/android/N2nController.kt <<'KTEOF'
package com.n2n.android

import com.n2n.mobile.Client
import com.n2n.mobile.Config
import java.util.concurrent.atomic.AtomicBoolean

object N2nController {

    private var client: Client? = null
    private val running = AtomicBoolean(false)

    fun isRunning(): Boolean = running.get()

    fun start(tunFd: Int, config: Config): String {
        if (running.get()) return "already running"

        val c = Client()
        c.setTunFD(tunFd.toLong())
        val err = c.start(config)
        if (err.isNotEmpty()) {
            return "start failed: $err"
        }
        client = c
        running.set(true)
        return ""
    }

    fun stop() {
        client?.stop()
        client = null
        running.set(false)
    }

    fun getStatus(): String = client?.status ?: "not running"
    fun getVirtualIP(): String = client?.virtualIP ?: ""
    fun getClientID(): String = client?.clientID ?: ""
    fun getPeersJSON(): String = client?.peersJSON ?: "[]"
}
KTEOF
echo "✅ N2nController.kt 已重写"

echo ""
echo "=== 4. 重写 UploadProgressListener.kt ==="
cat > app/src/main/java/com/n2n/android/UploadProgressListener.kt <<'KTEOF'
package com.n2n.android

import android.content.Context

/**
 * 上传进度监听（临时占位）
 *
 * gomobile 不支持导出 Go interface，暂时保留空类。
 * 后续方案：通过 Client.GetUploadProgressJSON() 轮询实现。
 */
class UploadProgressListener(private val ctx: Context) {
    // 暂时什么都不做
}
KTEOF
echo "✅ UploadProgressListener.kt 已重写"

echo ""
echo "=== 5. 删除 N2nVpnService.kt 里的 setProgressListener 调用 ==="
sed -i '/N2nController\.setProgressListener/d' \
  app/src/main/java/com/n2n/android/N2nVpnService.kt
echo "✅ N2nVpnService.kt 已处理"

echo ""
echo "=== 6. 删除 MainActivity.kt 里的 setProgressListener 调用 ==="
sed -i '/N2nController\.setProgressListener/d' \
  app/src/main/java/com/n2n/android/MainActivity.kt
echo "✅ MainActivity.kt 已处理"

echo ""
echo "=== 7. 验证 ==="
echo "--- 残留引用检查 ---"
RESIDUAL=$(grep -rn "setProgressListener\|ProgressListener" \
  mobile/mobile.go \
  mobile/internal/edge.go \
  app/src/main/java/com/n2n/android/N2nController.kt \
  app/src/main/java/com/n2n/android/N2nVpnService.kt \
  app/src/main/java/com/n2n/android/MainActivity.kt \
  2>/dev/null || true)

if [ -n "$RESIDUAL" ]; then
  echo "⚠️ 残留引用："
  echo "$RESIDUAL"
else
  echo "✅ 无残留引用"
fi

echo ""
echo "--- mobile.go package 名 ---"
head -3 mobile/mobile.go | grep "^package"

echo ""
echo "=== 完成 ==="
