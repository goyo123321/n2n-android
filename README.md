# n2n-android

基于 Cloudflare Workers 信令服务的 Android 客户端，配合 [edge-signal](https://github.com/goyo123321/edge-signal) 实现跨 NAT 的 P2P 虚拟组网。

通过 `VpnService` 建立 TUN 虚拟网卡，只接管组网网段（如 `10.64.0.0/24`），其他流量走系统默认。支持 **P2P 打洞 / TURN 中继 / WebSocket 中继** 三级降级，无需 root。

**内置 SOCKS5 入口**——与 VPN 并行运行，无需 root，可为特定 App 提供代理通道。

---

## 🌐 四端协同全景

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
   │ TUN 模式  │       │ SOCKS5   │        │VpnService│
   │ 系统级路由│       │ 代理模式 │        │ + SOCKS5 │
   │ 需 root  │       │ 无 root  │        │ 无 root  │
   └──────────┘       └──────────┘        └──────────┘
     Linux/macOS         Android              Android
     Windows             Termux                APK
     路由器

       LAN / P2P / TURN / WS  --  完全互通
```

### 三种客户端定位对比

| 维度 | n2n-go-client | n2n-termux | **n2n-android** |
|:---|:---|:---|:---|
| **运行环境** | Linux / macOS / Windows / 路由器 | Android Termux | **Android APK** |
| **数据面** | TUN 虚拟网卡 | SOCKS5 代理 | **VpnService + SOCKS5 双入口** |
| **权限要求** | root / 管理员 | 无 root | **无 root** |
| **应用透明** | ✅ 系统级 | ⚠️ 需 App 显式配 SOCKS5 | **✅ VpnService 系统级 + SOCKS5 可选** |
| **协议支持** | TCP / UDP / ICMP | 仅 TCP | **TCP / UDP / ICMP（VpnService）；TCP（SOCKS5）** |
| **使用方式** | `curl http://10.64.0.X` | `curl --socks5 127.0.0.1:1080 ...` | **同 TUN + 可选 SOCKS5** |
| **适用场景** | 服务器 / 桌面 / 路由器 | 临时测试 / 无 root 设备 | **日常手机使用** |

---

## 🔌 SOCKS5 入口（Android 独有）

### 为什么需要

`VpnService` 是**全局**的——一旦启动，所有 App 流量都走 TUN。但有时你只想让**部分 App** 访问组网资源，其他 App 保持走物理网络。

**场景举例**：

- 只想让 Firefox 访问内网 NAS，其他 App 不受影响
- 只想让 Termux 里的 SSH 走组网，其他 App 直连
- 需要同时挂 Clash 等其它代理（VpnService 独占，但 SOCKS5 可以共存）

此时可以**用 SOCKS5 入口**——不启用 VPN，只打开 SOCKS5 开关，让特定 App 通过 `127.0.0.1:1080` 访问组网服务。

### 与 VpnService 的关系

**两者可以独立使用，也可以同时运行**：

| 组合 | VpnService | SOCKS5 | 效果 |
|:---|:---|:---|:---|
| **仅 VpnService** | ✅ 开启 | ❌ 关闭 | 所有 App 走 VPN，应用透明 |
| **仅 SOCKS5** | ❌ 关闭 | ✅ 开启 | 只有配置了代理的 App 走组网 |
| **两者都开** | ✅ | ✅ | 双入口并存（互不干扰） |
| **两者都关** | ❌ | ❌ | 无组网（不可能同时关，至少开一个） |

**注意**：无论哪种组合，都需要 VPN 权限（因为 SOCKS5 底层走的是组网通道，而组网需要 TUN）。

### 使用步骤

#### 1. 打开开关

App 主界面 → **「🔌 SOCKS5 入口」卡片** → **打开开关**

自动填入默认监听地址 `127.0.0.1:1080`。

#### 2. 修改监听地址（可选）

| 地址 | 谁能访问 | 使用场景 |
|:---|:---|:---|
| `127.0.0.1:1080` | **仅本 App** | 默认，最安全 |
| `0.0.0.0:1080` | **局域网内所有设备** | 让其他设备借用手机的 SOCKS5 |

**如果要让同手机的 Termux 访问**（Android 10+ 有 loopback 隔离）→ **改成 `0.0.0.0:1080`**。

#### 3. 启动 VPN

点 **「启动 VPN」** → 授予 VPN 权限 → 状态变绿。

日志里会出现：

```
[SOCKS5] 已启用，监听 127.0.0.1:1080
```

#### 4. 使用 SOCKS5

**Firefox**：

```
about:preferences → 网络设置 → 手动代理
SOCKS 主机: 127.0.0.1
端口:     1080
☑ 使用 SOCKS v5 时代理 DNS
```

**Termux（同手机）**（需用 `0.0.0.0:1080`）：

```bash
# 查看手机局域网 IP
ifconfig | grep "inet "

# 通过手机的局域网 IP 访问 SOCKS5
curl --socks5 <手机IP>:1080 http://10.64.0.2:8080/
export ALL_PROXY=socks5://<手机IP>:1080
```

**其他 App**：在 App 的网络设置里找「代理」选项，填入 `127.0.0.1:1080`。

### 使用限制

| 限制 | 说明 |
|:---|:---|
| **仅支持 TCP** | SOCKS5 协议只代理 TCP，UDP 不通 |
| **目标必须是虚拟 IP** | 只能访问 `10.64.0.x` 段的对端设备，其他 IP 会被拒绝 |
| **不支持域名** | 客户端必须用虚拟 IP，不支持域名解析 |
| **运行中不可改** | VPN 运行期间开关会变灰，需先停止 VPN |

### 与同设备其他代理共存

- **与 Clash**：两者都监听不同端口即可。Clash 常用 `7890`，n2n SOCKS5 用 `1080`，无冲突
- **与 n2n-termux**：Termux 版也用 `127.0.0.1:1080`，**会冲突**！改其中一个端口即可
- **与 Surge/Quantumult**：类似，确保端口不冲突

---

## ✨ 特性

- **原生 VPN** — `VpnService` + TUN 内核栈，无需 root
- **SOCKS5 入口** — 与 VpnService 并行，为特定 App 提供代理
- **三级降级** — `P2P → TURN → WS`，连接永远可用
- **P2P 直连** — STUN 探测 + NAT 打洞，延迟低至 5ms
- **LAN 直连** — 同 WiFi 自动内网直连，延迟 <10ms
- **多目标打洞** — LAN + STUN 反射 + 观测 raddr 同时扫描，命中率大幅提升
- **CGNAT hairpin** — 同 STUN 出口 IP 时首次允许尝试，失败后服务端强制降级
- **UDP 保活** — 每 5 秒无条件刷新 STUN 映射，防止 CGNAT 端口漂移
- **P2P 通道保活** — 每 15 秒往对端发 N2NS PING 多目标探针
- **TURN 中继** — 支持 Cloudflare TURN + 自建 coturn（UDP / TCP / RFC6062）
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
   │   ├─ Socks5Server         SOCKS5 入口  │
   │   ├─ TUNDevice            TUN fd 包装  │
   │   └─ NetInfo              NetInfo      │
   └──────────────────────────────────────┘
            │
            ├─ VpnService → TUN → 系统流量
            │
            └─ SOCKS5 (127.0.0.1:1080) → 代理流量
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
| **UDP 保活** | 5 秒 | 5 个 STUN 服务器 | 保 socket 的 CGNAT 映射（无条件发送） |
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
6. 可选：打开 **🔌 SOCKS5 入口** 开关（默认关闭）
7. 点击 **启动 VPN** → 授予 VPN 权限
8. 状态变绿后，其它设备可通过虚拟 IP 互访

**虚拟 IP 分配**：`10.64.0.2` ~ `10.64.0.254`，由服务端自动分配。

---

## 📱 App 界面

### 主界面

| 卡片 | 内容 |
|:---|:---|
| **状态卡片** | 运行状态、虚拟 IP、网段、Client ID |
| **服务器卡片** | WSS 地址、优选 IP、连接密码 |
| **组网卡片** | 房间名、显示名、Client ID |
| **🔌 SOCKS5 卡片** | 开关 + 监听地址（默认 `127.0.0.1:1080`） |
| **节点列表** | 已连接节点 + 连接类型徽章（P2P / P2P~ / TURN / WS） |

### 菜单

| 菜单项 | 说明 |
|:---|:---|
| **日志** | 实时日志 + 复制 / 清空 / 自动滚动 |
| **主题** | 浅色 / 深色 / 跟随系统 |

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
| **SOCKS5 开关** | ❌ | 打开/关闭 SOCKS5 入口，默认关闭 |
| **SOCKS5 监听地址** | ❌ | `127.0.0.1:1080`（默认）或 `0.0.0.0:1080` |

### 深链

```
n2n://connect?url=wss://xxx.workers.dev&room=myroom&token=abc&ip=104.17.128.1&socks5=127.0.0.1:1080&auto=1
```

| 参数 | 说明 |
|:---|:---|
| `url` | WSS 地址 |
| `token` | 连接密码 |
| `room` | 房间名 |
| `cid` | Client ID |
| `name` | 显示名 |
| `ip` | 优选 IP |
| **`socks5`** | **SOCKS5 监听地址（填了则自动开启）** |
| `auto` | `1` 表示自动启动 VPN |

---

## 📊 连接类型徽章说明

App 节点列表和 Web 面板使用同一套徽章：

| 徽章 | 颜色 | 含义 |
|:---|:---|:---|
| `P2P` | 🟢 绿底 | P2P 直连，数据面已验证 |
| `P2P~` | 🟡 黄底虚线 | P2PAvailable 中间态（握手成功，数据面未验证） |
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

**注意**：SOCKS5 入口**不需要额外权限**——它监听 loopback，权限沿用 VpnService。

---

## 🚨 限制

### 规模

| 指标 | 上限 |
|:---|:---|
| 单房间设备数 | 253（受虚拟 IP 池限制） |
| 推荐规模 | 10 台以内 |
| 打洞协调复杂度 | O(N²) |

### SOCKS5 特有

| 项 | 说明 |
|:---|:---|
| **仅 TCP** | 不支持 UDP / ICMP |
| **目标必须是虚拟 IP** | `10.64.0.x` 段，不支持域名 |
| **Android 10+ loopback 隔离** | 同设备其他 App 无法访问 `127.0.0.1:1080`，需改 `0.0.0.0:1080` |
| **运行中不可修改** | 需先停止 VPN |

### 兼容性

| 项 | 说明 |
|:---|:---|
| 最低版本 | Android 8.0（API 26） |
| 目标版本 | Android 14（API 34） |
| ABI | arm64-v8a / armeabi-v7a / x86_64 |
| IPv6 | 部分支持（P2P 打洞暂只支持 IPv4） |

### 与其他 VPN 冲突

同一台 Android 设备**同时只能启用一个 VpnService**。与 Clash / 其他 VPN 类 App 需二选一。

**但 SOCKS5 入口可以与 Clash 共存**——只要端口不冲突。

---

## ❓ 常见问题

### Q: SOCKS5 和 VPN 有什么区别？

| 项 | VpnService | SOCKS5 |
|:---|:---|:---|
| **覆盖范围** | 全设备（系统级） | 仅配置了代理的 App |
| **协议** | TCP / UDP / ICMP | **仅 TCP** |
| **应用透明** | ✅ 无需配置 | ⚠️ 需 App 显式配代理 |
| **权限** | VPN 权限 | 无额外权限 |
| **适用** | 全局组网 | 定向代理 |

**推荐**：日常用 VpnService，特殊场景（如与其他代理共存）用 SOCKS5。

### Q: SOCKS5 打开后为什么还需要 VPN 权限？

因为组网底层**必须用 TUN 虚拟网卡**（SOCKS5 只是一个入口，把流量导入组网通道）。所以即使你只想要 SOCKS5，也要走 VpnService 的权限。

**如果连 TUN 都不想开** → 用 [n2n-termux](https://github.com/goyo123321/n2n-termux)（SOCKS5 模式，无需 VPN 权限）。

### Q: 打开 SOCKS5 后其他 App 会变慢吗？

**不会**。SOCKS5 **只监听 `127.0.0.1:1080`**，不接管任何系统流量。只有**主动配置了 SOCKS5 代理**的 App 才会走组网。

同时，VpnService 只接管 `10.64.0.0/24`，其他流量走系统默认。

### Q: Firefox 怎么配 SOCKS5？

```
about:preferences
  → 拉到底部 → 网络设置 → 设置...
  → 手动代理配置
    ○ SOCKS 主机: 127.0.0.1
    ○ 端口:      1080
    ○ SOCKS v5
    ☑ 使用 SOCKS v5 时代理 DNS 查询
  → 确定
```

**注意**：访问组网资源要用**虚拟 IP**（`10.64.0.x`），不要用域名。

### Q: Termux 里怎么用手机的 SOCKS5？

**Android 10+ 的 loopback 隔离**让 Termux 访问不到其他 App 的 `127.0.0.1:1080`。需要：

1. **把 App 里的 SOCKS5 监听地址改成 `0.0.0.0:1080`**
2. **在 Termux 里用手机的局域网 IP**：

```bash
# 查手机的局域网 IP
ifconfig | grep "inet " | grep -v 127.0.0.1

# 通过手机的局域网 IP 访问 SOCKS5
curl --socks5 192.168.1.100:1080 http://10.64.0.2:8080/

# 或者设置环境变量
export ALL_PROXY=socks5://192.168.1.100:1080
```

**安全提示**：`0.0.0.0:1080` 会让同一 WiFi 下的所有设备都能访问你的 SOCKS5，**建议只在可信网络用**。

### Q: SOCKS5 卡片的开关点不动？

**检查 VPN 是否在运行**。运行时开关会变灰（防止配置冲突），需要先「停止 VPN」。

### Q: 目标 IP 不是 10.64.0.x 会怎么样？

SOCKS5 会**拒绝**并返回 `Host unreachable`。因为 `findPeerByVIP` 只识别虚拟 IP，其他 IP 没有对应的 peer。

### Q: 与 Clash / Surge 冲突吗？

**不冲突**（只要端口不同）。Clash 默认用 `7890`，n2n SOCKS5 用 `1080`，互不干扰。

**唯一冲突点**：Clash 也可能用 `1080` 端口（部分配置）。检查一下，冲突就改 n2n 的监听端口。

### Q: 三端可以互相访问吗？

**可以，全部支持**。见上文「四端协同全景」。

- **PC ↔ Android**：都是 TUN 数据面，直接访问虚拟 IP
- **PC ↔ Termux**：PC 走 TUN，Termux 走 SOCKS5，透明互通
- **Android ↔ Termux**：同 PC ↔ Termux
- **Android 的 SOCKS5 入口 ↔ 其他设备**：可以，但只能代理 TCP

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

1. `publicEndpoint` 为空 → NAT 探测失败
2. `natType=EasyNAT` 且是单样本 → 服务端探测太旧
3. 端口差 > 100 → CGNAT 映射漂移严重

### Q: P2P 建立后过一段时间断了？

**CGNAT 映射或会话被回收**。检查：

1. 日志有没有 `[Keepalive] 启动` 和 `[P2P-Keepalive] 启动`
2. `publicEndpoint` 端口是不是又漂移了
3. P2P 通道保活是不是每 15 秒发一次 N2NS PING

### Q: 面板显示 `p2p-avail` 不是 `p2p`？

**`p2p-avail`** = 打洞握手成功，但 PONG 或真实数据帧还没到。正常 1-2 秒内会升级到 `p2p`。**一直停在 `p2p-avail`** → 30 秒后自动降级。

### Q: 同 WiFi 延迟波动很大（100ms+）？

**已修复**。原因是两端同时向 LAN + 公网发包，导致对端 `peer.UDPAddr` 被公网地址覆盖，回程走 CGNAT 回环。

**修复后**：
- 若 peer 最近 5 秒从私网发过包 → **只发 LAN**（走内网直达，~8ms）
- 收到包更新 `peer.UDPAddr` 时 → **LAN 优先**（不被公网覆盖）

现在同 WiFi 场景延迟稳定在 **8~15ms**，标准差 <2ms。

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
