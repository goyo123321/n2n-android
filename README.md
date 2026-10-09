# n2n-android

基于 Cloudflare Workers 信令服务的 Android 客户端，**配合 edge-signal 实现跨 NAT 的 P2P 虚拟组网**。

通过 `VpnService` 建立 TUN 虚拟网卡，只接管组网网段（`10.64.0.0/24`），其他流量走系统默认。支持 **LAN 直连 / P2P 打洞 / TURN 中继 / WebSocket 中继** 四级降级，无需 root。

## ✨ 特性

- **原生 VPN** — `VpnService` + TUN 内核栈，无需 root
- **四级降级** — `LAN → P2P → TURN → WS`，连接永远可用
- **同 WiFi 直连** — 检测到同子网直接用局域网 IP，延迟 < 5ms
- **多出口上报** — 同时上报 WiFi / 蜂窝 / 以太网所有局域网 IP
- **P2P 直连** — STUN 探测 + NAT 打洞，延迟低至 5ms
- **TURN 中继** — UDP (RFC 5766) + TCP (RFC 6062) 双栈，支持 Cloudflare TURN 和自建 coturn
- **WS 中继兜底** — TURN 不可用时自动降级到 Worker 转发
- **优选 IP** — 支持 Cloudflare 优选 IP，绕过 DNS 解析加速信令
- **断线重连** — WebSocket 指数退避重连
- **深链支持** — `n2n://connect?url=...&token=...` 一键组网
- **主题切换** — 浅色 / 深色 / 跟随系统
- **连接可视化** — 节点列表带 P2P / TURN / WS 徽章

## 🏗️ 架构

```
   Android 客户端
   ┌──────────────────────────────────────┐
   │   Kotlin 层                            │
   │   ├─ MainActivity         UI          │
   │   ├─ N2nVpnService        VPN 生命周期 │
   │   ├─ N2nController        Go 桥接      │
   │   └─ LogActivity          日志页      │
   ├──────────────────────────────────────┤
   │   Go 层（gomobile AAR）                │
   │   ├─ Edge                 主控制器     │
   │   ├─ WSTransport          信令         │
   │   ├─ TURNClient           TURN 主控    │
   │   ├─ TUNDevice            TUN fd 包装  │
   │   └─ NetInfo              LAN 探测     │
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
            ├─ 优先级 1: LAN 直连（同子网，延迟 < 5ms）
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

首次构建约 1-2 分钟。输出：`app/libs/n2nclient.aar`

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
| **状态卡片** | 运行状态、虚拟 IP、Client ID |
| **服务器卡片** | WSS 地址、优选 IP、连接密码 |
| **组网卡片** | 房间名、显示名、Client ID |
| **节点列表** | 已连接节点 + 连接类型徽章（P2P / TURN / WS） |

### 菜单

| 菜单项 | 说明 |
|:---|:---|
| **日志** | 实时日志 + 复制 / 清空 / 自动滚动 |
| **主题** | 浅色 / 深色 / 跟随系统 |

### 日志页

- 每秒刷新一次
- 自动滚动开关
- 支持复制全部 / 清空日志

## ⚙️ 配置项

| 字段 | 必填 | 说明 |
|:---|:---|:---|
| WSS 地址 | ✅ | 信令服务器地址，`wss://` 或 `ws://` 开头 |
| 房间名 | ✅ | 只允许字母 / 数字 / 下划线 / 短横线 |
| 连接密码 | ❌ | 对应服务端 `CONNECT_TOKEN`。**留空 = Worker 未启用校验** |
| 优选 IP | ❌ | Cloudflare 优选 IP，格式 `IP` 或 `IP:端口`。留空走 DNS |
| 显示名 | ❌ | 显示在面板上的名称，默认 "Android" |
| Client ID | ❌ | 留空自动生成，多设备建议手填固定值 |

### 深链

支持通过 URL 一键配置并启动：

```
n2n://connect?url=wss://xxx.workers.dev&room=myroom&token=abc&ip=104.17.128.1&auto=1
```

| 参数 | 说明 |
|:---|:---|
| `url` | WSS 地址 |
| `token` | 连接密码（对应服务端 `CONNECT_TOKEN`） |
| `room` | 房间名 |
| `cid` | Client ID |
| `name` | 显示名 |
| `ip` | 优选 IP |
| `auto` | `1` 表示自动启动 VPN |

也支持通过 Intent 传参：

```kotlin
val intent = Intent(context, MainActivity::class.java).apply {
    putExtra("signaling_url", "wss://xxx.workers.dev")
    putExtra("room_id", "myroom")
    putExtra("auto_start", true)
}
```

## 🌐 网络路径

### 四级降级

```
1. LAN 直连 —— 同子网时用局域网 IP，延迟 < 5ms
   ↓ 失败（AP 隔离 / 不同网段）
2. P2P 打洞 —— STUN + NAT 打洞，延迟 10~50ms
   ↓ 失败（对称 NAT / CGNAT 池化）
3. TURN 中继 —— UDP / TCP 双栈中继，延迟 30~100ms
   ↓ 失败（TURN 未配置或不可达）
4. WS 中继 —— 走 Cloudflare Worker，延迟 100~300ms
```

### LAN 直连原理

**客户端启动时收集本机所有局域网 IP**（WiFi / 蜂窝 / 以太网），通过 `p2p_metadata` 上报。服务端广播 `joined` 时带上对端的 `lanIps`。

**收到 `joined` 后**：客户端本地对比 `sameSubnet(myLanIPs, peerLanIPs)`：

- 前 3 段相同 → **直接用局域网 IP 作为 UDP 目标地址** + 标记 P2P
- 前 3 段不同 → 等待打洞指令

**同时服务端也会判断**：双方 `lanIps` 有交集时**跳过打洞**（客户端本地已经直连，服务端无需下发指令）。

**支持多出口**：手机同时有 WiFi + 4G 时，两个网段的 IP 都会上报。对端匹配任意一个即可。

### TUN 只接管组网网段

```
VpnService.Builder
  .addAddress("10.64.0.2", 24)
  .addRoute("10.64.0.0", 24)   ← 只接管虚拟网段
```

**效果**：微信、Chrome、系统更新等流量**完全不受影响**，只有发往 `10.64.0.x` 的包进入 VPN。

### protected socket

Android 一旦 VPN 启动，**所有 socket 默认走 VPN**——包括 P2P 打洞的 UDP socket 和 STUN socket。这会形成死循环（自己的 UDP 包被自己的 TUN 捕获）。

**解决**：所有 P2P 相关的 socket 创建后立即 `protect(fd)`，让它们走物理网络。

## 📋 启动日志

启动成功后：

```
n2n-go-client 启动
[LAN] 本机局域网出口: [192.168.10.2]
[P2P] 使用 protected UDP fd=97（绕过 VPN）
[P2P] UDP 监听端口 49918
[NAT] 使用 protected STUN socket fd=105
[WS] 优选 IP: 23.227.38.65:443 (SNI=url.jyce.kdns.fr)
[WS] 连接 wss://url.jyce.kdns.fr/ws/default-room?cid=android-xxx
分配虚拟 IP: 10.64.0.3
[信令] 服务端看到的本机出口 IP: 120.229.199.61
[信令] 上报 p2p_metadata: natType=EasyNAT publicEndpoint=120.239.134.13:18899 lanIps=[192.168.10.2] udpPort=49918 multiExit=false
[TURN] ✅ UDP 就绪: 111.171.194.230:62755
[TURN] ✅ RFC 6062 TCP allocation 就绪: 111.171.194.230:49493
[NAT] EasyNAT pub=120.239.134.13:18899
[Edge] 已启动 clientId=android-xxx room=default-room
```

其他节点上线时：

```
[信令] joined: from=android-yyy vip=10.64.0.4 lanIPs=[192.168.10.3] lanPort=54321
[LAN] android-yyy 与我同网段，直连 192.168.10.3:54321
[连接] android-yyy → P2P 直连
```

**LAN 直连时**：从 `joined` 到 P2P 建立大约 100ms（本地判断，无需打洞）。

**走 TURN 时**：

```
[NAT-HOLE] ❌ 失败 role=0 target=120.230.253.98 attempts=5
[连接] android-yyy → TURN 中继
```

## 🔧 从源码构建

### 前置要求

- Go 1.21+
- Android NDK 26.1.10909125
- Gradle（通过 `./gradlew` 自动下载）

### 构建 AAR

```bash
export ANDROID_NDK_HOME=$HOME/Android/Sdk/ndk/26.1.10909125
./build-aar.sh
```

`build-aar.sh` 会自动：

1. 检查 Go 版本
2. 安装 gomobile（首次）
3. 执行 `go mod tidy`
4. 执行 `gomobile init`
5. 用 `-javapkg com.n2n` 生成 AAR（确保包名是 `com.n2n.mobile`）
6. 验证生成的 AAR 含 `com/n2n/mobile/Client.class`

### 构建 APK

```bash
./gradlew assembleDebug    # Debug
./gradlew assembleRelease  # Release（需签名配置）
```

### GitHub Actions 自动构建

见仓库 `.github/workflows/`。

## 📁 项目结构

```
n2n-android/
├── mobile/                      # Go 客户端（gomobile 目标）
│   ├── mobile.go               # gomobile 桥接层
│   ├── internal/
│   │   ├── edge.go             # 客户端主控制器
│   │   ├── config.go           # 配置结构
│   │   ├── ws_transport.go     # 信令 WebSocket（含断线重连）
│   │   ├── turn_client.go      # TURN 客户端主控
│   │   ├── turn_lite.go        # TURN UDP (RFC 5766)
│   │   ├── turn_lite_tcp.go    # TURN TCP (RFC 6062)
│   │   ├── nat_probe.go        # STUN NAT 探测
│   │   ├── nathole_executor.go # 打洞指令执行
│   │   ├── netinfo.go          # 局域网出口探测
│   │   ├── relay_fallback.go   # 四级降级管理器
│   │   ├── tun.go              # TUN fd 包装
│   │   ├── protector.go        # VpnService.protect 桥
│   │   ├── logger.go           # 日志环形缓冲
│   │   └── helpers.go          # 工具函数
│   ├── go.mod
│   ├── go.sum
│   └── build-aar.sh
├── app/                         # Android App
│   ├── src/main/
│   │   ├── java/com/n2n/android/
│   │   │   ├── MainActivity.kt
│   │   │   ├── N2nVpnService.kt
│   │   │   ├── N2nController.kt
│   │   │   ├── LogActivity.kt
│   │   │   └── Prefs.kt
│   │   ├── res/
│   │   │   ├── layout/         # 布局
│   │   │   ├── drawable/       # 图标
│   │   │   ├── values/         # 颜色 / 字符串 / 主题（浅色）
│   │   │   ├── values-night/   # 深色主题
│   │   │   ├── mipmap-*/       # 应用图标
│   │   │   └── xml/            # 配置
│   │   └── AndroidManifest.xml
│   ├── libs/
│   │   └── n2nclient.aar       # 由 build-aar.sh 生成
│   ├── build.gradle.kts
│   └── proguard-rules.pro
└── README.md
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
| `ACCESS_WIFI_STATE` | WiFi 检测 |
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

1. **先确认是不是同 WiFi** —— 同 WiFi 下应该走 LAN 直连，日志里会有 `[LAN] xxx 与我同网段`。如果没看到，检查两端是否真的在同一子网。
2. **不同 WiFi 时**，看日志里的 `[NAT]` 行：
   - `EasyNAT` → 正常情况下能打洞
   - `HardNAT` + `multiExit=true` → CGNAT 池化，打洞大概率失败，靠 TURN 兜底
3. **两台都在对称 NAT 后** → 只能靠 TURN / WS 中继

### Q: 同 WiFi 下为什么还走 TURN？

**检查日志**：

```
[LAN] 本机局域网出口: [192.168.10.2]   ← 是否为空？
[信令] joined: from=xxx lanIPs=[...]    ← 对端是否上报了？
```

- **本机 lanIps 为空** → `/proc/net/route` 或 `net.Interfaces()` 没读到，可能是 ROM 限制
- **对端 lanIps 为空** → 同上
- **两者都有但仍走 TURN** → 可能是路由器 AP 隔离（同网段但不通），或子网不是 /24（如 /16 大网段）

### Q: 连接后无法访问外网？

**正常行为**。TUN 只接管 `10.64.0.0/24`——外网流量走系统默认。如果外网也断了，说明有**其他 VPN 正在运行**（比如 Clash、系统自带的 VPN），两者冲突。

### Q: 耗电严重？

1. 关闭其他 VPN 类 App
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
3. 通过 `adb logcat` 查看 `AndroidRuntime` / `N2nVpnService` 相关错误
4. 确认 `values-night/colors.xml` 存在（缺失会导致深色模式崩溃）

### Q: 服务端看到的我的 IP 和 STUN 结果不一样？

**说明你在 CGNAT 池化网络里**——运营商有多个公网出口，TCP 和 UDP 从不同出口出去。日志会出现：

```
[NAT] ⚠️ WS/STUN 出口不一致：WS=120.229.199.61 STUN=120.239.134.13 —— CGNAT 池化，打洞大概率失败
```

**这种情况下 P2P 打洞基本不可能成功**，客户端会自动走 TURN 兜底。**不是 bug，是网络限制**。

## 📄 License

MIT

## 🔗 相关项目

- [edge-signal](https://github.com/goyo123321/edge-signal) — 服务端（Cloudflare Workers）
- [n2n-go-client](https://github.com/goyo123321/n2n-go-client) — 跨平台客户端（桌面 / 路由器 / 服务器）
- [pion/stun](https://github.com/pion/stun) — STUN 协议
- [gorilla/websocket](https://github.com/gorilla/websocket) — WebSocket
- [gomobile](https://pkg.go.dev/golang.org/x/mobile) — Go/Kotlin 桥接
