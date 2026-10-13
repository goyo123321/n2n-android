# n2n-android

基于 Cloudflare Workers 信令服务的 Android 客户端，配合 [edge-signal](https://github.com/goyo123321/edge-signal) 实现跨 NAT 的 P2P 虚拟组网。

通过 `VpnService` 建立 TUN 虚拟网卡，只接管组网网段（如 `10.64.0.0/24`），其他流量走系统默认。支持 **P2P 打洞 / TURN 中继 / WebSocket 中继** 三级降级，无需 root。

---

## 🌐 三端协同全景

`edge-signal` 服务端 + 三个客户端构成完整的异地组网系统：

```
              ┌───────────────────────────────────────────┐
              │      edge-signal (Cloudflare Worker)      │
              │  ├─ 信令路由                              │
              │  ├─ NAT 打洞协调                          │
              │  ├─ TURN 凭证下发                         │
              │  └─ WS 中继（最后兜底）                   │
              └──────────────┬────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
   n2n-go-client       n2n-termux          n2n-android
   ┌──────────┐       ┌──────────┐        ┌──────────┐
   │ TUN 模式  │       │ SOCKS5   │        │ VpnService│
   │ 系统级路由│       │ 代理模式 │        │ 无 root  │
   │ 需 root  │       │ 无 root  │        │          │
   └──────────┘       └──────────┘        └──────────┘
     Linux/macOS         Android              Android
     Windows             Termux                APK
     路由器

       LAN / P2P / TURN / WS  --  完全互通
```

### 三种客户端定位对比

| 维度 | n2n-go-client | n2n-termux | n2n-android |
|:---|:---|:---|:---|
| **运行环境** | Linux / macOS / Windows / 路由器 | Android Termux | Android APK |
| **数据面** | TUN 虚拟网卡 | SOCKS5 代理 | VpnService（TUN） |
| **权限要求** | root / 管理员 | 无 root | 无 root |
| **应用透明** | ✅ 系统级 | ⚠️ 需 App 显式配 SOCKS5 | ✅ 系统级 |
| **协议支持** | TCP / UDP / ICMP | 仅 TCP | TCP / UDP / ICMP |
| **使用方式** | `curl http://10.64.0.X` | `curl --socks5 127.0.0.1:1080 ...` | 同 TUN |
| **适用场景** | 服务器 / 桌面 / 路由器 | 临时测试 / 无 root 设备 | 日常使用（手机） |

---

## 🔧 Termux 与 root 的运用

### 场景 1：Termux 无 root（默认）

使用 **[n2n-termux](https://github.com/goyo123321/n2n-termux)** —— SOCKS5 模式：

- ✅ 不需要 root
- ⚠️ 仅支持 TCP（ICMP 和 UDP 需绕行）
- ⚠️ 每个 App 需显式配置 SOCKS5（`curl --socks5`、`ALL_PROXY` 等）

```bash
# 启动
~/.n2n-termux/start.sh

# 使用（需要显式走 SOCKS5）
curl --socks5 127.0.0.1:1080 http://10.64.0.3:8080/
export ALL_PROXY=socks5://127.0.0.1:1080
```

### 场景 2：Termux + root

**关键认识**：**Termux 无 root 时，Android 内核拒绝创建 TUN。但有了 root，就可以绕过这个限制。**

具体两种用法：

#### 用法 A：用 root 直接运行 n2n-go-client

```bash
# 在 Termux 里（需要 root）
su -c "SIGNALING_URL=... ROOM_ID=myroom CLIENT_ID=termux-root \
  ./n2n-go-client-linux-arm64"
```

**优势**：
- ✅ 完整的 TUN 数据面（TCP / UDP / ICMP 全支持）
- ✅ 应用透明（无需改每个 App 的代理配置）
- ✅ 与 PC / Android 客户端**完全对等**

**限制**：
- 需要设备已 root（Magisk / KernelSU 等）
- Termux 里要以 root 运行：`su -c` 或 `tsu`（Termux 的 `tsu` 包）

#### 用法 B：用 proot-distro 在非 root 下跑

```bash
pkg install proot-distro
proot-distro install debian
proot-distro login debian
# 装 n2n-go-client，但 TUN 依然需要 root——proot 不能绕过
```

**proot 解决不了 TUN 权限**。`/dev/net/tun` 是内核级的，proot 用户态拦截不生效。

**结论**：要 TUN 必须真的 root。

### 场景 3：三种 Android 客户端的协作

**同一台 Android 设备可以同时运行多个客户端吗？**

**不建议** —— VPN 冲突。原因：

- `VpnService` 是**独占**的，一台设备同时只能有一个活跃 VPN
- SOCKS5 模式的 Termux 与 VpnService 模式的 Android APK 可以**共存**（因为 SOCKS5 不抢 VPN 权限）
- 具体组合：

| 组合 | 是否推荐 | 说明 |
|:---|:---|:---|
| n2n-android（VpnService） | ✅ 推荐 | 主用方式 |
| n2n-termux（SOCKS5） | ✅ 可共存 | 与 VpnService 一起运行时，**只有 VpnService 走 TUN**，Termux 走 SOCKS5，两者**互不冲突** |
| n2n-android + n2n-termux | ⚠️ 可共存 | VpnService 只接管组网网段，Termux 的 SOCKS5 走 `127.0.0.1`，无冲突 |
| **n2n-android × 2** | ❌ 不行 | 两个 VpnService 会抢 VPN 权限 |
| Clash / 其他 VPN | ⚠️ 需协调 | 与 Clash 冲突，只能同时启用一个 |

**推荐配置**：

- 手机日常使用 → **n2n-android**
- 需要临时调试 / 跑脚本 → **额外**用 n2n-termux 的 SOCKS5（与 VpnService 共存）
- 设备已 root 且想要最优性能 → **Termux + root 跑 n2n-go-client**

### 场景 4：数据面差异与互通

**三端数据面协议完全独立**，但**服务端与 P2P 通道层用同一套协议**（N2NS 帧 + 打洞 + TURN 中继），所以完全互通：

| 主动方 | 被动方 | 数据面路径 |
|:---|:---|:---|
| PC（TUN） | Termux（SOCKS5） | PC 发 N2NS → Termux 收到后转发到本地 TCP |
| Termux（SOCKS5） | PC（TUN） | Termux 发 N2NS → PC 收到后写入 TUN |
| PC（TUN） | Android（VpnService） | 直接写 N2NS 帧（两端都是 TUN） |
| Android（VpnService） | Termux（SOCKS5） | Android 发 N2NS → Termux 侧转发到本地 TCP |
| Termux（SOCKS5） | Android（VpnService） | Termux 发 N2NS → Android 收到后写入 TUN |

**关键**：**N2NS 帧格式在所有客户端完全一致**，所以任意两端可以直接互通。

### 场景 5：LAN 直连（同 WiFi）

三端**都会收集 LAN 地址**：

- PC：`net.Interfaces()` + UDP connect 骗内核
- Termux：`net.Interfaces()`（Android 有限制，主要靠 UDP connect）
- Android：`net.Interfaces()`（过滤 tun 接口）+ UDP connect

**同 WiFi 场景**：两端在打洞指令里会带上 LAN 候选，**优先扫 LAN 端口**，命中后直接走内网，延迟 <5ms。

### 场景 6：CGNAT hairpin（同运营商）

两端在同一运营商 CGNAT 后面时（STUN 出口 IP 相同）：

1. **首次**：服务端允许一次 hairpin 尝试
2. **失败后**：服务端下发 `force_fallback` → 三端立即降级到 TURN
3. **已成功**：跳过

**三端行为完全一致**。

---

## ✨ 特性

- **原生 VPN** — `VpnService` + TUN 内核栈，无需 root
- **三级降级** — `P2P → TURN → WS`，连接永远可用
- **P2P 直连** — STUN 探测 + NAT 打洞，延迟低至 5ms
- **LAN 直连** — 同 WiFi 自动内网直连
- **CGNAT hairpin** — 同 STUN 出口 IP 时尝试端口扫描 + probe 回发，失败后服务端强制降级
- **P2P 通道保活** — 每 15 秒往对端发 N2NS PING 多目标探针，防止空闲时通道被回收
- **TURN 中继** — 支持 Cloudflare TURN 和自建 coturn（UDP / TCP / RFC6062）
- **WS 中继兜底** — TURN 不可用时自动降级到 Worker 转发
- **WS 应用层隧道** — 支持 PC / Termux 通过 WS 中继访问 Android 上的服务
- **优选 IP** — 支持 Cloudflare 优选 IP，绕过 DNS 解析加速信令
- **断线重连** — WebSocket 指数退避重连 + 90 秒读超时检测下行僵死
- **深链支持** — `n2n://connect?url=...&token=...` 一键组网
- **主题切换** — 浅色 / 深色 / 跟随系统
- **连接可视化** — 节点列表带 P2P / TURN / WS 徽章

---

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
   │   ├─ P2PStreamManager     N2NS 流     │
   │   ├─ AppStreamManager     WS 隧道     │
   │   ├─ TUNDevice            TUN fd 包装  │
   │   └─ NetInfo              NetInfo      │
   └──────────────────────────────────────┘
            │
            ▼
   ┌────────────────────────────────────────┐
   │   Cloudflare Worker (edge-signal)       │
   └────────────────────────────────────────┘
            │
            ├─ 优先级 1: LAN 直连（同 WiFi）
            ├─ 优先级 2: P2P 打洞（含 CGNAT hairpin）
            ├─ 优先级 3: TURN 中继
            └─ 优先级 4: WebSocket 中继
```

---

## 🔥 保活机制

| 层 | 间隔 | 目标 | 作用 |
|:---|:---|:---|:---|
| **UDP 保活** | 5 秒 | 3 个 STUN 服务器 | 保 socket 的 CGNAT 映射 |
| **P2P 通道保活** | 15 秒 | 每个 P2P 对端 | 保 A→B 会话 |
| **WS 心跳** | 20 秒 | 服务端 | 保 WS 连接 |

**流量代价**：
- UDP 保活：~50 MB/天
- P2P 保活：~180 KB/天 × 对端数
- 电量代价：几乎可忽略

**真实数据帧天然刷新 CGNAT 映射**（最强保活）。保活机制只在**空闲时**起兜底作用。

---

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

输出：`app/libs/n2nclient.aar`

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

---

## 🌐 典型使用场景

### 场景 A：PC ↔ Android 手机

**PC 访问手机上的服务**（如 ADB / HTTP 服务）：

```bash
# Android 上跑服务
# （例如 Termux 里 python3 -m http.server 8080 --bind 0.0.0.0）

# PC 端直接访问
curl http://10.64.0.x:8080/
```

### 场景 B：三端混合组网

```
PC-A (10.64.0.2)  ←→  Android-B (10.64.0.3)  ←→  Termux-C (10.64.0.4)
```

- **PC 访问 Android**：直接 `curl http://10.64.0.3:8080/`
- **Android 访问 PC**：直接 `curl http://10.64.0.2:8080/`
- **Termux 访问 PC**：`curl --socks5 127.0.0.1:1080 http://10.64.0.2:8080/`
- **PC 访问 Termux**：`curl http://10.64.0.4:8080/`（Termux 端服务监听 `0.0.0.0`）

### 场景 C：Android 设备做网关

Android 手机作为访问其他节点的"边缘客户端"：

```
[Android-A]──组网──[PC-B]──局域网络──[数据库服务器]
```

Android 通过组网访问 PC，PC 转发到真正的内网资源。

### 场景 D：Termux + Root 做全功能节点

在已 root 的 Android 设备上，用 Termux 跑 n2n-go-client，**与 PC 完全对等**：

```bash
# Termux + root
su -c "SIGNALING_URL=wss://xxx ROOM_ID=myroom CLIENT_ID=my-phone \
  ./n2n-go-client-linux-arm64"
```

- ✅ 完整 TUN 数据面（TCP / UDP / ICMP）
- ✅ 应用透明（系统级路由）
- ✅ 与 PC 完全对等的使用体验

---

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

```
n2n://connect?url=wss://xxx.workers.dev&room=myroom&token=abc&ip=104.17.128.1&auto=1
```

| 参数 | 说明 |
|:---|:---|
| `url` | WSS 地址 |
| `token` | 连接密码 |
| `room` | 房间名 |
| `cid` | Client ID |
| `name` | 显示名 |
| `ip` | 优选 IP |
| `auto` | `1` 表示自动启动 VPN |

---

## 📊 连接类型徽章说明

App 节点列表和 Web 面板使用同一套徽章：

| 徽章 | 颜色 | 含义 |
|:---|:---|:---|
| `P2P` | 🟢 绿底 | P2P 直连，数据面已验证 |
| `P2P~` | 🟡 黄底虚线 | P2PAvailable 中间态（打洞握手成功，数据面未验证） |
| `TURN` | 🔵 蓝底 | TURN 中继 |
| `WS` | 🟡 黄底 | WS 中继（最后兜底） |

---

## 🔐 权限说明

| 权限 | 用途 |
|:---|:---|
| `INTERNET` | 网络通信 |
| `ACCESS_NETWORK_STATE` | 网络状态检测 |
| `FOREGROUND_SERVICE` | 前台服务 |
| `FOREGROUND_SERVICE_SPECIAL_USE` | VPN 前台服务 |
| `POST_NOTIFICATIONS` | 通知栏状态显示 |
| `WAKE_LOCK` | 保持运行 |
| `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS` | 电池优化白名单 |

---

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

### 与其他 VPN 冲突

同一台 Android 设备**同时只能启用一个 VpnService**。与 Clash / 其他 VPN 类 App 需二选一。

**SOCKS5 模式的 n2n-termux 可以与 n2n-android 共存**——两者走不同数据面（Termux 走 `127.0.0.1`，不抢 VPN 权限）。

---

## ❓ 常见问题

### Q: 三端可以互相访问吗？

**可以，全部支持**。见上文"三端协同全景"和"典型使用场景"。

- **PC ↔ Android**：都是 TUN 数据面，直接访问虚拟 IP
- **PC ↔ Termux**：PC 走 TUN，Termux 走 SOCKS5，透明互通
- **Android ↔ Termux**：同 PC ↔ Termux

### Q: Termux 无 root 能用 TUN 吗？

**不能**。Android 内核拒绝非特权 App 创建 TUN 网卡。

- **Termux 无 root** → 用 [n2n-termux](https://github.com/goyo123321/n2n-termux)（SOCKS5 模式）
- **Termux + root** → 可直接跑 n2n-go-client（TUN 模式），与 PC 完全对等
- **不 root 且要 TUN** → 装 n2n-android APK（用 VpnService）

### Q: 同一台设备能同时跑 n2n-android 和 n2n-termux 吗？

**可以**，两者不冲突：

- n2n-android 用 VpnService（走 TUN）
- n2n-termux 用 SOCKS5（走 `127.0.0.1`）
- TUN 只接管组网网段，SOCKS5 端口不受影响

但**同一类型只能开一个**：两个 VpnService 会抢权限。

### Q: `ping 10.64.0.3` 不通？

Android 的 TUN 对 ICMP 支持不完整，部分 ROM 直接丢弃 TUN 上写回的 ICMP reply。

**用 TCP 测**：

```bash
curl -v --max-time 5 http://10.64.0.3:8080/
```

### Q: 打洞一直失败？

看日志：

```
[信令] 上报 p2p_metadata: natType=? publicEndpoint=?
[NAT-HOLE] 公网候选 ? 个
```

1. `publicEndpoint` 为空 → NAT 探测失败，STUN 服务器被封
2. `natType=EasyNAT` 且是单样本 → 服务端探测太旧
3. `公网候选 7 个` → 服务端 `coordinator.js` 太旧
4. 端口差 > 100 → CGNAT 映射漂移严重

### Q: P2P 建立后过一段时间断了？

**CGNAT 映射或会话被回收**。检查：

1. 日志有没有 `[Keepalive] 启动` 和 `[P2P-Keepalive] 启动`
2. `publicEndpoint` 端口是不是又漂移了
3. P2P 通道保活是不是每 15 秒发一次 N2NS PING

### Q: 面板显示 `p2p-avail` 不是 `p2p`？

**`p2p-avail`** = 打洞握手成功，但 PONG 或真实数据帧还没到。

正常 1-2 秒内会升级到 `p2p`：

```
[P2P] 从 xxx 收到 Punch → P2PAvailable（等待数据帧验证）
[P2P] 收到 PONG → P2P 直连（双向通道已验证）
```

**一直停在 `p2p-avail`** → 30 秒后自动降级到 TURN/WS。

### Q: 服务端 `force_fallback` 收到后没反应？

检查日志：

```
[信令] 收到 force_fallback: 1 个 peer, reason=server-forced
[连接] xxx → TURN 中继 (force: server-forced)
```

如果只看到第一行没有第二行 → 客户端版本太旧。

### Q: PC 通过 WS 中继访问 Android 服务失败？

**这是 Android 侧曾有的缺口**——`app_stream.go` 已在本版本补齐。确认：

1. 两端都是最新版本
2. Android 日志里能看到 `[STREAM] 收到 open xxx from pc-xxx`
3. PC 端日志里有 `[STREAM] xxx → xxx:port 打开`

### Q: 耗电严重？

1. 关闭其他 VPN 类 App（VpnService 独占）
2. 在系统设置里将 App 加入电池优化白名单
3. 长时间不使用请在 App 内停止 VPN

**注意**：保活（5 秒 STUN + 15 秒 P2P）的电量开销可忽略，不是耗电原因。

---

## 📄 License

MIT

## 🔗 相关项目

- [edge-signal](https://github.com/goyo123321/edge-signal) — 服务端（Cloudflare Workers）
- [n2n-go-client](https://github.com/goyo123321/n2n-go-client) — 跨平台客户端（桌面 / 路由器 / 服务器）
- [n2n-termux](https://github.com/goyo123321/n2n-termux) — Termux SOCKS5 客户端
- [pion/stun](https://github.com/pion/stun) — STUN 协议
- [gorilla/websocket](https://github.com/gorilla/websocket) — WebSocket
- [gomobile](https://pkg.go.dev/golang.org/x/mobile) — Go/Kotlin 桥接
