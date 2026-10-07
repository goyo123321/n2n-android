# n2n-android

基于 Cloudflare Workers 信令服务的 Android 客户端，**配合 edge-signal 实现跨 NAT 的 P2P 虚拟组网**。

通过 WebSocket 信令建立连接，支持 NAT 打洞协调、P2P 直连、TURN 中继和 WebSocket 中继回退，配合 gVisor 用户态网络栈实现**无需 root 的全流量接管**。

## ✨ 特性

- **原生 VPN** — `VpnService` + gVisor netstack，全流量接管，无需 root
- **四级降级** — `LAN → P2P → TURN → WS`，保证连接永远可用
- **同 WiFi 直连** — 检测到同网关时直接走局域网，延迟最低
- **共享盘** — 每台设备暴露 HTTP 文件服务，组网内互访
- **出口代理** — 通过 Workers 出口访问被墙/CGNAT 封堵的目标
- **智能分流** — geosite + geoip 规则，国内直连国外走代理
- **双 DNS** — 国内域名走国内 DoH，国外域名走境外 DoH
- **日志可视化** — App 内实时查看运行日志
- **深链支持** — `n2n://connect?url=...&auto=1` 一键组网
- **主题切换** — 浅色 / 深色 / 跟随系统

## 🏗️ 架构

```
   Android 客户端
   ┌──────────────────────────────────────┐
   │   Kotlin 层                            │
   │   ├─ MainActivity         UI          │
   │   ├─ N2nVpnService        VPN 生命周期 │
   │   ├─ N2nController        Go 桥接      │
   │   └─ FileManager/Log/Share 子页面     │
   ├──────────────────────────────────────┤
   │   Go 层（gomobile AAR）                │
   │   ├─ Edge                 主控制器     │
   │   ├─ WSTransport          信令         │
   │   ├─ WSOutbound           Workers 出口 │
   │   ├─ TURNClient           TURN 主控    │
   │   ├─ NetstackHost         gVisor + 共享盘│
   │   ├─ Router              分流规则       │
   │   └─ DNSProxy             DoH 分流      │
   └──────────────────────────────────────┘
            │
            ▼
   ┌────────────────────────────────────────┐
   │   Cloudflare Worker (edge-signal)       │
   │   ├─ 信令路由                           │
   │   ├─ NAT 打洞协调                       │
   │   ├─ 下发 TURN 凭证                     │
   │   └─ WS 中继转发（最后兜底）            │
   └────────────────────────────────────────┘
            │
            ├─ 优先级 1: LAN 直连（同网关，延迟最低）
            │  A ←──────────────→ B
            │
            ├─ 优先级 2: P2P 打洞（零服务器带宽）
            │  A ←──────────────→ B
            │
            ├─ 优先级 3: TURN 中继（少量服务器带宽）
            │  A ──→ TURN ──→ B
            │
            └─ 优先级 4: WebSocket 中继（最后兜底）
               A ──→ Worker ──→ B
```

## 🚀 快速开始

### 前置要求

- Go 1.21+
- Android NDK 26.1.10909125
- Android Studio（可选）
- 已部署的 [edge-signal](https://github.com/goyo123321/edge-signal) Worker

### 一、构建 AAR

```bash
export ANDROID_NDK_HOME=$HOME/Android/Sdk/ndk/26.1.10909125
./build-aar.sh
```

首次构建约 3-5 分钟（需要编译 gVisor）。输出：`app/libs/n2nclient.aar`

**如果没有 NDK**，可通过 Android Studio 安装：

```
SDK Manager → SDK Tools → NDK (Side by side) → 勾选 26.1.10909125
```

### 二、编译 APK

```bash
./gradlew assembleDebug
```

输出：`app/build/outputs/apk/debug/app-debug.apk`

### 三、安装

```bash
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

### 四、使用

1. 打开 App
2. 填写 **WSS 地址**（edge-signal 部署地址，`https://` → `wss://`）
3. 填写 **房间名**（同一房间的设备互相组网）
4. 可选：填写 **优选 IP**（Cloudflare 优选 IP，加速连接）
5. 可选：填写 **连接密码**（对应服务端 `CONNECT_TOKEN`）
6. 点击 **启动 VPN** → 授予 VPN 权限
7. 状态变绿后，其它设备可通过虚拟 IP 互访

**虚拟 IP 分配**：`10.64.0.2` ~ `10.64.0.254`，由服务端自动分配。

## 📱 App 界面

### 主界面

| 卡片 | 内容 |
|:---|:---|
| **状态卡片** | 运行状态、虚拟 IP、Client ID、共享盘 URL |
| **服务器卡片** | WSS 地址、优选 IP、连接密码 |
| **组网卡片** | 房间名、显示名、Client ID |
| **共享盘卡片** | 当前目录、管理文件、打开共享盘 |
| **节点列表** | 已连接节点 + 连接类型（p2p / TURN / ws） |

### 菜单

| 菜单项 | 说明 |
|:---|:---|
| **日志** | 实时日志 + 复制 / 清空 |
| **主题** | 浅色 / 深色 / 跟随系统 |

### 日志页

- 每秒刷新一次
- 自动滚动开关
- 支持复制全部 / 清空日志

### 文件管理

- 浏览共享盘目录
- 上传（SAF 多选）
- 新建文件夹
- 删除（长按触发）
- 分享 / 预览 / 导出到 `Download/n2n-share/`

## ⚙️ 配置项

| 字段 | 必填 | 说明 |
|:---|:---|:---|
| WSS 地址 | ✅ | 信令服务器地址，`wss://` 或 `ws://` 开头 |
| 房间名 | ✅ | 只允许字母 / 数字 / 下划线 / 短横线 |
| 连接密码 | ❌ | 对应服务端 `CONNECT_TOKEN` |
| 优选 IP | ❌ | Cloudflare 优选 IP，格式 `IP` 或 `IP:端口` |
| 显示名 | ❌ | 显示在面板上的名称，默认 "Android" |
| Client ID | ❌ | 留空自动生成，多设备建议手填固定值 |

### 深链

支持通过 URL 一键配置并启动：

```
n2n://connect?url=wss://xxx.workers.dev&room=myroom&ip=104.17.128.1&auto=1
```

| 参数 | 说明 |
|:---|:---|
| `url` | WSS 地址 |
| `ip` | 优选 IP |
| `room` | 房间名 |
| `cid` | Client ID |
| `name` | 显示名 |
| `token` | 连接密码 |
| `auto` | `1` 表示自动启动 VPN |

也支持通过 Intent 传参：

```kotlin
val intent = Intent(context, MainActivity::class.java).apply {
    putExtra("signaling_url", "wss://xxx.workers.dev")
    putExtra("room_id", "myroom")
    putExtra("auto_start", true)
}
```

## 🔧 共享盘

### 默认目录

| 系统版本 | 路径 |
|:---|:---|
| Android 10+ | `Android/media/com.n2n.android/shared/` |
| Android 9- | `<externalFilesDir>/shared/` |

**Android 10+ 的目录特点**：

- MT 管理器 / USB 连电脑可直接访问（无需任何权限）
- 微信 / QQ 文件选择器能看到
- 系统媒体扫描器会自动索引

### 访问方式

| 方式 | 地址 |
|:---|:---|
| 本机浏览器 | `http://10.64.0.x:9090/` |
| 组网内其它设备 | `http://<对方虚拟IP>:9090/` |
| 电脑（USB 连接） | 直接拖拽文件 |

### HTTP API

| 端点 | 说明 |
|:---|:---|
| `GET /` | 文件浏览页面 |
| `GET /api/list?path=/` | 列出目录 |
| `GET /api/download?path=/file.txt` | 下载文件 |
| `POST /api/upload?path=/file.txt` | 上传文件 |
| `DELETE /api/delete?path=/file.txt` | 删除文件 |
| `POST /api/mkdir?path=/dir` | 创建目录 |
| `GET /api/node_info` | 节点信息（JSON） |

## 🌐 网络分流

### 分流规则

| 规则 | 动作 |
|:---|:---|
| 目标 IP 在 `10.64.0.0/24` | `p2p`：走 n2n 组网 |
| 目标 IP 在私网 / 环回 | `direct`：走物理网络 |
| 目标域名匹配 geosite:cn | `direct` |
| 目标 IP 匹配 geoip:cn | `direct` |
| 其他 | `proxy`：走 Workers 出口 |

### DNS 分流

| 域名类型 | DoH 服务器 |
|:---|:---|
| 国内域名 | 阿里 DoH / 腾讯 DoH / 360 DoH |
| 国外域名 | Cloudflare DoH / Google DoH |

## 📁 项目结构

```
mobile/                        # Go 客户端（gomobile 目标）
├── mobile.go                 # gomobile 桥接层
├── internal/
│   ├── edge.go               # 客户端主控制器
│   ├── config.go             # 配置结构
│   ├── ws_transport.go       # 信令 WebSocket
│   ├── ws_outbound.go        # Workers 出口 Mux 客户端
│   ├── turn_client.go        # TURN 客户端主控
│   ├── turn_lite.go          # TURN UDP (RFC 5766)
│   ├── turn_lite_tcp.go      # TURN TCP (RFC 6062)
│   ├── nat_probe.go          # STUN NAT 探测
│   ├── nathole_executor.go   # 打洞指令执行
│   ├── netstack.go           # gVisor netstack + 共享盘
│   ├── dns.go                # DoH 分流
│   ├── router.go             # 路由规则
│   ├── geoip_cn.go           # 国内 IP 判定
│   ├── geosite_cn.go         # 国内域名判定
│   ├── netinfo.go            # 局域网信息
│   ├── relay_fallback.go     # 中继管理器
│   ├── http_proxy.go         # 本地 HTTP 代理
│   ├── tun.go                # TUN fd 包装
│   ├── protector.go          # VpnService.protect 桥
│   ├── stream_conn.go        # 流 → net.Conn 适配
│   ├── progress.go           # 上传进度回调
│   └── logger.go             # 日志环形缓冲
├── data/
│   ├── geoip_cn.txt          # 国内 IP 段
│   └── geosite_cn.txt        # 国内域名
├── go.mod
├── go.sum
└── build-aar.sh

app/                           # Android App
├── src/main/
│   ├── java/com/n2n/android/
│   │   ├── MainActivity.kt
│   │   ├── N2nVpnService.kt
│   │   ├── N2nController.kt
│   │   ├── FileManagerActivity.kt
│   │   ├── FileAdapter.kt
│   │   ├── LogActivity.kt
│   │   ├── ShareWebActivity.kt
│   │   ├── ShareDirManager.kt
│   │   └── Prefs.kt
│   ├── res/
│   │   ├── layout/           # 布局
│   │   ├── drawable/         # 图标
│   │   ├── values/           # 颜色 / 字符串 / 主题
│   │   ├── mipmap-*/         # 应用图标
│   │   └── xml/              # FileProvider 路径
│   └── AndroidManifest.xml
├── libs/
│   └── n2nclient.aar         # 由 build-aar.sh 生成
├── build.gradle.kts
├── proguard-rules.pro
└── gradlew.bat
```

## 🔄 数据生命周期

| 事件 | 行为 |
|:---|:---|
| App 启动 | 读取 Prefs，恢复上次配置 |
| 点击启动 | FetchVirtualIP → 建立 TUN → 起 Go Edge |
| 客户端断开 | 服务端标记 `online: false`，虚拟 IP 保留 30 分钟 |
| App 停止 VPN | 关闭 TUN / UDP / STUN socket，停 Go Edge |
| App 卸载 | 清理所有本地文件（含日志） |

## 🔐 权限说明

| 权限 | 用途 |
|:---|:---|
| `INTERNET` | 网络通信 |
| `ACCESS_NETWORK_STATE` | 网络状态检测 |
| `FOREGROUND_SERVICE` | 前台服务 |
| `FOREGROUND_SERVICE_SPECIAL_USE` | VPN 前台服务 |
| `POST_NOTIFICATIONS` | 通知栏状态显示 |
| `WAKE_LOCK` | 保持运行 |
| `ACCESS_WIFI_STATE` | WiFi 检测（同网段判断） |
| `CHANGE_WIFI_STATE` | WiFi Lock |
| `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS` | 电池优化白名单 |

## 🚨 限制

### 规模

| 指标 | 上限 |
|:---|:---|
| 单房间设备数 | 253（受虚拟 IP 池限制） |
| 推荐规模 | 10 台以内 |
| 打洞协调复杂度 | O(N²) |

### 兼容性

| 项 | 说明 |
|:---|:---|
| 最低版本 | Android 8.0（API 26） |
| 目标版本 | Android 14（API 34） |
| ABI | arm64-v8a / armeabi-v7a / x86_64 |
| IPv6 | 部分支持（P2P 打洞暂只支持 IPv4） |

## ❓ 常见问题

### Q: 打洞一直失败？

1. 检查服务端是否配置了 TURN（无 TURN 时对称 NAT 无法打洞）
2. 两台设备可能都在对称 NAT 后，此时只能靠 TURN / WS 中继
3. 查看 App 日志确认 NAT 类型（`EasyNAT` / `HardNAT`）

### Q: 连接后无法访问外网？

1. 检查 Workers 出口代理是否就绪（日志中应有 `[WSOut] Workers 出口代理就绪`）
2. 若目标是 CF CDN IP，且 TURN 未就绪，会直接失败——让服务端配置 TURN 后重试

### Q: 共享盘打不开？

1. 检查虚拟 IP 是否已分配（状态卡片显示）
2. 从另一台设备访问 `http://<虚拟IP>:9090/`
3. 若只在本机访问，用 `http://<本机虚拟IP>:9090/`

### Q: 耗电严重？

1. 关闭其他 VPN 类 App（会与本 App 抢流量）
2. 在系统设置里将 App 加入电池优化白名单（App 会主动引导）
3. 长时间不使用请在 App 内停止 VPN

### Q: 编译 AAR 报 "NDK 路径不存在"？

```bash
# Linux/macOS
export ANDROID_NDK_HOME=$HOME/Android/Sdk/ndk/26.1.10909125

# Windows Git Bash
export ANDROID_NDK_HOME=/c/Users/<you>/AppData/Local/Android/Sdk/ndk/26.1.10909125
```

NDK 可通过 Android Studio 安装：`SDK Manager → SDK Tools → NDK (Side by side)`

### Q: App 启动后闪退？

1. 检查是否授予了 VPN 权限
2. 检查 `app/libs/n2nclient.aar` 是否存在（未构建 AAR 会导致 `ClassNotFoundException`）
3. 通过 Logcat 查看 `N2nVpnService` 相关错误

## 📄 License

MIT

## 🔗 相关项目

- [edge-signal](https://github.com/goyo123321/edge-signal) — 服务端（Cloudflare Workers）
- [n2n-go-client](https://github.com/goyo123321/n2n-go-client) — 跨平台客户端（桌面 / 路由器 / 服务器）
- [gVisor](https://gvisor.dev/) — 用户态网络栈
- [pion/stun](https://github.com/pion/stun) — STUN 协议
- [gorilla/websocket](https://github.com/gorilla/websocket) — WebSocket
- [gomobile](https://pkg.go.dev/golang.org/x/mobile) — Go/Kotlin 桥接
