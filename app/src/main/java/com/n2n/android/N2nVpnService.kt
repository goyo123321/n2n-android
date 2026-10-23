package com.n2n.android

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ServiceInfo
import android.net.ConnectivityManager
import android.net.LinkProperties
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.net.VpnService
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import android.os.SystemClock
import android.util.Log
import androidx.core.app.NotificationCompat
import com.n2n.mobile.Client
import com.n2n.mobile.Config
import com.n2n.mobile.Protector
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.Inet6Address
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

class N2nVpnService : VpnService() {

    companion object {
        private const val TAG = "N2nVpnService"
        private const val NOTIF_CHANNEL_ID = "n2n_vpn"
        private const val NOTIF_ID = 1001

        private const val DEFAULT_VIRTUAL_NETWORK = "10.64.0.0/24"
        private const val TUN_MTU = 1400

        // ★ 新增：网络回调去抖窗口（与 Go 层 edge_network.go 的 1.5s 对齐）
        private const val NETWORK_DEBOUNCE_MS = 1500L

        const val ACTION_START = "com.n2n.android.START"
        const val ACTION_STOP = "com.n2n.android.STOP"

        const val EXTRA_SIGNALING_URL = "signaling_url"
        const val EXTRA_ROOM_ID = "room_id"
        const val EXTRA_CLIENT_ID = "client_id"
        const val EXTRA_NODE_NAME = "node_name"
        const val EXTRA_CONNECT_TOKEN = "connect_token"
        const val EXTRA_PREFERRED_IP = "preferred_ip"
        const val EXTRA_SOCKS5_LISTEN = "socks5_listen"

        private val crashHandlerInstalled = AtomicBoolean(false)
    }

    private var tunInterface: ParcelFileDescriptor? = null
    @Volatile private var started = false
    private val starting = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private val handler = Handler(Looper.getMainLooper())

    @Volatile private var destroyed = false
    @Volatile private var notificationActive = false

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null

    private var protectedUdpSocket: DatagramSocket? = null
    private var protectedStunSocket: DatagramSocket? = null

    // ============================================================
    // 网络变化监听（去抖 + IPv4 优先 + 忽略 IPv6 link-local）
    // ============================================================
    //
    // 两级检测：
    //   1. onAvailable：网络类型变化（WiFi ↔ Cellular ↔ Ethernet）
    //   2. onLinkPropertiesChanged：网关变化（WiFi1 → WiFi2，同类型）
    //
    // ★ 关键修复：
    //   Android 同一 WiFi 有 IPv4 + IPv6 link-local (fe80::1) 双默认路由，
    //   onLinkPropertiesChanged 会在两者间反复触发 → 4 秒 6 次 → 状态机
    //   和数据面反复被打断。
    //
    //   本版本：
    //   - pickGateway() 优先 IPv4，没有才用 IPv6 非 link-local
    //   - 与「上次通知出去的 gateway」比较，相同直接跳过
    //   - 1.5 秒去抖窗口（与 Go 层 edge_network.go 合并延迟对齐）
    private var networkCallback: ConnectivityManager.NetworkCallback? = null
    @Volatile private var lastNetworkType = ""
    @Volatile private var lastNotifiedGateway = ""
    @Volatile private var lastNetworkTypeNotifyAt = 0L
    @Volatile private var lastGatewayNotifyAt = 0L

    private val tzReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action == Intent.ACTION_TIMEZONE_CHANGED) {
                N2nController.applySystemTimezone()
            }
        }
    }

    inner class ServiceProtector : Protector {
        override fun protect(fd: Long): Boolean {
            return try {
                this@N2nVpnService.protect(fd.toInt())
            } catch (t: Throwable) {
                Log.w(TAG, "protect($fd) 失败: ${t.message}")
                false
            }
        }
    }

    private fun installCrashHandler() {
        if (!crashHandlerInstalled.compareAndSet(false, true)) return
        val default = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            try {
                val sw = StringWriter()
                throwable.printStackTrace(PrintWriter(sw))
                ktLog("========== [CRASH] Java ==========")
                ktLog("线程: ${thread.name}")
                ktLog("类型: ${throwable.javaClass.name}")
                ktLog("消息: ${throwable.message}")
                ktLog(sw.toString())
                ktLog("==================================")
            } catch (_: Throwable) {}
            try { default?.uncaughtException(thread, throwable) } catch (_: Throwable) {}
        }
    }

    private fun ktLog(msg: String) {
        try {
            val ts = SimpleDateFormat("HH:mm:ss", Locale.US).format(Date())
            val line = "[$ts] [K] $msg"

            val candidates = mutableListOf<File>()
            try { candidates.add(File(filesDir, "n2n.log")) } catch (_: Throwable) {}
            candidates.add(File("/data/user/0/$packageName/files/n2n.log"))
            candidates.add(File("/data/data/$packageName/files/n2n.log"))

            var written = false
            for (f in candidates) {
                try {
                    f.parentFile?.mkdirs()
                    f.appendText("$line\n")
                    written = true
                    break
                } catch (_: Throwable) {}
            }
            if (!written) Log.e(TAG, "ktLog 全部路径写入失败")
        } catch (_: Throwable) {}
        Log.i(TAG, msg)
    }

    private val updateIpRunnable = object : Runnable {
        private var attempts = 0
        override fun run() {
            if (!notificationActive) return
            try {
                attempts++
                val ip = N2nController.getVirtualIP()
                if (ip.isNotEmpty()) {
                    updateNotification("已连接 · 虚拟 IP $ip")
                    Log.i(TAG, "VPN 虚拟 IP: $ip")
                    notificationActive = false
                } else if (attempts < 30) {
                    updateNotification("正在连接... (${attempts}s)")
                    if (notificationActive) handler.postDelayed(this, 1000)
                } else {
                    updateNotification("正在连接... (${attempts}s)")
                    if (notificationActive) handler.postDelayed(this, 3000)
                }
            } catch (t: Throwable) {
                Log.e(TAG, "updateIpRunnable failed", t)
                notificationActive = false
            }
        }
    }

    override fun onCreate() {
        super.onCreate()

        N2nController.applyLogFilePath(this)
        N2nController.applySystemTimezone()
        installCrashHandler()

        try {
            registerReceiver(
                tzReceiver,
                IntentFilter(Intent.ACTION_TIMEZONE_CHANGED)
            )
        } catch (_: Throwable) {}

        registerNetworkCallback()

        ktLog("onCreate: 进程启动/Service 创建")
    }

    // ============================================================
    // 网络变化回调注册/注销
    // ============================================================

    private fun registerNetworkCallback() {
        try {
            val cm = getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            val cb = object : ConnectivityManager.NetworkCallback() {

                override fun onAvailable(network: Network) {
                    try {
                        val caps = cm.getNetworkCapabilities(network) ?: return
                        // VPN 自身跳过，避免 tun0 触发循环
                        if (caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) return

                        val type = when {
                            caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> "wifi"
                            caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> "cellular"
                            caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> "ethernet"
                            else -> "unknown"
                        }
                        val prev = lastNetworkType
                        if (type == prev) return
                        lastNetworkType = type

                        if (prev.isEmpty()) {
                            ktLog("[Network] 初始网络: $type")
                            return
                        }

                        // ★ 去抖：1.5 秒内不重复通知
                        val now = SystemClock.elapsedRealtime()
                        if (now - lastNetworkTypeNotifyAt < NETWORK_DEBOUNCE_MS) {
                            ktLog("[Network] 类型 $prev → $type 与上次 < 1.5s，去抖跳过")
                            return
                        }
                        lastNetworkTypeNotifyAt = now

                        ktLog("[Network] 类型变化: $prev → $type，通知 Go 层")
                        N2nController.notifyNetworkChanged(type)
                    } catch (t: Throwable) {
                        Log.w(TAG, "NetworkCallback.onAvailable failed", t)
                    }
                }

                override fun onLinkPropertiesChanged(network: Network, lp: LinkProperties) {
                    try {
                        val caps = cm.getNetworkCapabilities(network)
                        if (caps?.hasTransport(NetworkCapabilities.TRANSPORT_VPN) == true) return

                        // ★ pickGateway 优先 IPv4，忽略 IPv6 link-local
                        val gateway = pickGateway(lp) ?: return

                        // ★ 与「上次通知出去的 gateway」比，相同直接跳过
                        //   旧版用 lastGateway 会导致 A→B→A 的 B 也被通知
                        if (gateway == lastNotifiedGateway) return

                        // 首次
                        if (lastNotifiedGateway.isEmpty()) {
                            lastNotifiedGateway = gateway
                            ktLog("[Network] 初始网关: $gateway")
                            return
                        }

                        // ★ 去抖：1.5 秒内不重复通知
                        val now = SystemClock.elapsedRealtime()
                        if (now - lastGatewayNotifyAt < NETWORK_DEBOUNCE_MS) {
                            ktLog("[Network] 网关 $lastNotifiedGateway → $gateway 与上次 < 1.5s，去抖跳过")
                            return
                        }

                        val prev = lastNotifiedGateway
                        lastNotifiedGateway = gateway
                        lastGatewayNotifyAt = now

                        ktLog("[Network] 网关变化: $prev → $gateway，通知 Go 层")
                        N2nController.notifyNetworkChanged("gateway:$gateway")
                    } catch (t: Throwable) {
                        Log.w(TAG, "NetworkCallback.onLinkPropertiesChanged failed", t)
                    }
                }

                override fun onLost(network: Network) {
                    // 紧随 onAvailable 触发，不必单独处理
                }
            }
            cm.registerNetworkCallback(
                NetworkRequest.Builder()
                    .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                    .build(),
                cb
            )
            networkCallback = cb
        } catch (t: Throwable) {
            ktLog("[Network] 注册 NetworkCallback 失败: ${t.message}")
        }
    }

    // pickGateway 优先取 IPv4 默认网关；没有 IPv4 再取 IPv6 非 link-local。
    //
    // 为什么不直接用 firstOrNull{isDefaultRoute}：
    //   Android 同一 WiFi 有 IPv4 + IPv6 link-local (fe80::1) 双默认路由，
    //   onLinkPropertiesChanged 会在两者间反复触发 → 4 秒 6 次 → 数据面被打断。
    //
    // 为什么 IPv6 要排除 link-local：
    //   fe80:: 只在本地链路有效，WiFi1 和 WiFi2 的 fe80::1 完全可能相同，
    //   它的变化不能作为「网络切换」的信号。
    //
    // 为什么还要接受 IPv6 非 link-local：
    //   部分 IPv6-only 环境（NAT64、企业网）没有 IPv4 默认网关，
    //   此时 IPv6 global/ULA 是唯一信号，不能漏。
    private fun pickGateway(lp: LinkProperties): String? {
        val routes = lp.routes.filter { it.isDefaultRoute && it.gateway != null }
        routes.firstOrNull { it.gateway is Inet4Address }
            ?.gateway?.hostAddress?.let { return it }
        routes.firstOrNull {
            val gw = it.gateway
            gw is Inet6Address && !gw.isLinkLocalAddress
        }?.gateway?.hostAddress?.let { return it }
        return null
    }

    private fun unregisterNetworkCallback() {
        try {
            networkCallback?.let {
                (getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager)
                    .unregisterNetworkCallback(it)
            }
        } catch (t: Throwable) {
            Log.w(TAG, "unregisterNetworkCallback failed", t)
        }
        networkCallback = null
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        ktLog("onStartCommand action=${intent?.action} started=$started")

        if (!safeStartForeground()) {
            ktLog("startForeground 失败，stopSelf")
            stopSelf()
            return START_NOT_STICKY
        }

        try {
            when (intent?.action) {
                ACTION_START -> handleStart(intent)
                ACTION_STOP -> handleStop()
                else -> {
                    ktLog("unknown action or null intent, stopSelf")
                    stopSelf()
                    return START_NOT_STICKY
                }
            }
        } catch (t: Throwable) {
            ktLog("onStartCommand 崩溃: ${t.message}")
            Log.e(TAG, "onStartCommand crash", t)
            try { handleStop() } catch (_: Throwable) {}
        }
        return START_NOT_STICKY
    }

    private fun safeStartForeground(): Boolean {
        return try {
            val notif = buildNotification("n2n 组网启动中")
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                startForeground(
                    NOTIF_ID,
                    notif,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
                )
            } else {
                startForeground(NOTIF_ID, notif)
            }
            true
        } catch (t: Throwable) {
            Log.e(TAG, "safeStartForeground failed", t)
            false
        }
    }

    override fun onRevoke() {
        ktLog("onRevoke: VPN 权限被撤销")
        try { handleStop() } catch (_: Throwable) {}
        try { super.onRevoke() } catch (_: Throwable) {}
    }

    // ============================================================
    // handleStart
    // ============================================================

    private fun handleStart(intent: Intent) {
        if (started) {
            ktLog("handleStart: 已启动，跳过")
            updateNotification("已连接")
            return
        }

        if (N2nController.isRunning() || N2nController.isStopping()) {
            ktLog("handleStart: Controller running=${N2nController.isRunning()} stopping=${N2nController.isStopping()}，异步等待 500ms 后重试")
            if (N2nController.isRunning()) {
                try { N2nController.stop() } catch (t: Throwable) {
                    Log.e(TAG, "force stop failed", t)
                }
            }
            val retryIntent = Intent(intent)
            handler.postDelayed({
                if (destroyed || stopping.get()) return@postDelayed
                if (N2nController.isRunning() || N2nController.isStopping()) {
                    ktLog("handleStart: 等待后仍 running/stopping，放弃启动")
                    stopVpn()
                    return@postDelayed
                }
                handleStart(retryIntent)
            }, 500)
            return
        }

        if (!starting.compareAndSet(false, true)) {
            ktLog("handleStart: 已有启动流程在跑，忽略")
            updateNotification("正在启动中...")
            return
        }

        ktLog("handleStart 开始")

        val signalingUrl = intent.getStringExtra(EXTRA_SIGNALING_URL) ?: run {
            ktLog("handleStart: 无 signalingUrl，退出")
            starting.set(false)
            updateNotification("配置错误：缺少信令地址")
            stopVpn()
            return
        }
        val preferredIp = intent.getStringExtra(EXTRA_PREFERRED_IP) ?: ""
        val roomId = intent.getStringExtra(EXTRA_ROOM_ID) ?: "default-room"

        val clientId = (intent.getStringExtra(EXTRA_CLIENT_ID)?.takeIf { it.isNotBlank() })
            ?: Prefs.loadOrCreateClientId(this)

        val nodeName = intent.getStringExtra(EXTRA_NODE_NAME) ?: "Android"
        val connectToken = intent.getStringExtra(EXTRA_CONNECT_TOKEN) ?: ""
        val socks5Listen = intent.getStringExtra(EXTRA_SOCKS5_LISTEN) ?: ""

        ktLog("参数读取完成 room=$roomId cid=$clientId token=${if (connectToken.isEmpty()) "<empty>" else "***"} socks5=${if (socks5Listen.isEmpty()) "<disabled>" else socks5Listen}")

        try {
            updateNotification("正在获取虚拟 IP...")
            acquireLocks()
            ktLog("acquireLocks 完成")
        } catch (t: Throwable) {
            ktLog("acquireLocks 失败: ${t.message}")
            starting.set(false)
            stopVpn()
            return
        }

        val config = try {
            Config().apply {
                setSignalingURL(signalingUrl)
                setRoomID(roomId)
                setClientID(clientId)
                setNodeName(nodeName)
                setConnectToken(connectToken)
                setPreferredIP(preferredIp)
                setPreferredPort(443)
                setSocks5Listen(socks5Listen)
            }
        } catch (t: Throwable) {
            ktLog("Config 构造失败: ${t.message}")
            starting.set(false)
            stopVpn()
            return
        }
        ktLog("Config 构造完成")

        Thread {
            var vip = ""
            var virtualNetwork = DEFAULT_VIRTUAL_NETWORK
            var fetchOk = false
            var tmpClient: Client? = null
            try {
                ktLog("开始 FetchVIP")
                tmpClient = Client()
                val raw = tmpClient.fetchVirtualIP(config) ?: ""
                val parsed = parseReadyPayload(raw)
                vip = parsed.first
                virtualNetwork = parsed.second
                fetchOk = vip.isNotEmpty()
                ktLog("FetchVIP 返回: vip='$vip' cidr='$virtualNetwork'（原始='$raw'）")
            } catch (t: Throwable) {
                ktLog("FetchVIP 崩溃: ${t.message}")
            } finally {
                try { tmpClient?.stop() } catch (_: Throwable) {}
            }

            if (!fetchOk) {
                handler.post {
                    if (destroyed || stopping.get()) return@post
                    updateNotification("获取虚拟 IP 失败")
                    handler.postDelayed({ stopVpn() }, 3000)
                }
                return@Thread
            }

            handler.post {
                try {
                    if (destroyed || stopping.get()) return@post

                    if (vip.isEmpty()) {
                        ktLog("VIP 为空，退出")
                        updateNotification("获取虚拟 IP 失败")
                        handler.postDelayed({ stopVpn() }, 3000)
                        return@post
                    }

                    ktLog("开始建立 TUN")
                    val pfd = buildTunInterface(vip, virtualNetwork)
                    if (pfd == null) {
                        ktLog("TUN 建立失败")
                        updateNotification("建立 TUN 失败")
                        handler.postDelayed({ stopVpn() }, 3000)
                        return@post
                    }
                    tunInterface = pfd
                    val tunFd = pfd.detachFd()
                    ktLog("TUN fd=$tunFd")

                    ktLog("创建 protected UDP socket")
                    val udpFd = createProtectedUdpSocket()
                    ktLog("UDP fd=$udpFd")
                    if (udpFd <= 0) {
                        ktLog("UDP fd 无效，退出")
                        updateNotification("UDP socket 创建失败")
                        handler.postDelayed({ stopVpn() }, 3000)
                        return@post
                    }

                    ktLog("创建 protected STUN socket")
                    val stunFd = createProtectedStunSocket()
                    ktLog("STUN fd=$stunFd")

                    ktLog("调用 startAsync")
                    N2nController.startAsync(
                        tunFd, udpFd, stunFd, config, ServiceProtector()
                    ) { err ->
                        handler.post {
                            try {
                                if (destroyed || stopping.get()) {
                                    ktLog("startAsync 回调到达时服务已停止，忽略 (err=$err)")
                                    return@post
                                }
                                if (err.isNotEmpty()) {
                                    ktLog("startAsync 失败: $err")
                                    updateNotification("启动失败: $err")
                                    handler.postDelayed({ stopVpn() }, 3000)
                                } else {
                                    ktLog("startAsync 成功")
                                    started = true
                                    starting.set(false)
                                    notificationActive = true
                                    try { Prefs.markVpnStartedOnce(this@N2nVpnService) } catch (_: Throwable) {}
                                    handler.post(updateIpRunnable)
                                }
                            } catch (t: Throwable) {
                                Log.e(TAG, "startAsync callback failed", t)
                            }
                        }
                    }
                } catch (t: Throwable) {
                    ktLog("handler.post 崩溃: ${t.message}")
                    try { updateNotification("启动失败: ${t.message}") } catch (_: Throwable) {}
                    handler.postDelayed({ stopVpn() }, 3000)
                }
            }
        }.apply { name = "n2n-fetchvip" }.start()
    }

    private fun parseReadyPayload(raw: String): Pair<String, String> {
        if (raw.isEmpty()) return "" to DEFAULT_VIRTUAL_NETWORK

        val idx = raw.indexOf('|')
        if (idx < 0) {
            ktLog("fetchVirtualIP 无 CIDR（老服务端？），回退 $DEFAULT_VIRTUAL_NETWORK")
            return raw.trim() to DEFAULT_VIRTUAL_NETWORK
        }

        val vip = raw.substring(0, idx).trim()
        val cidr = raw.substring(idx + 1).trim()

        return vip to (cidr.ifEmpty { DEFAULT_VIRTUAL_NETWORK })
    }

    private fun extractFdFromDatagramSocket(socket: DatagramSocket): Int {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            try {
                val pfd = ParcelFileDescriptor.fromDatagramSocket(socket)
                if (pfd != null) {
                    val fd = pfd.detachFd()
                    if (fd > 0) return fd
                }
            } catch (_: Throwable) {}
        }

        try {
            val m = ParcelFileDescriptor::class.java.getDeclaredMethod(
                "fromDatagramSocket", DatagramSocket::class.java
            )
            m.isAccessible = true
            val pfd = m.invoke(null, socket) as? ParcelFileDescriptor
            if (pfd != null) {
                val fd = pfd.detachFd()
                if (fd > 0) return fd
            }
        } catch (_: Throwable) {}

        return -1
    }

    private fun createProtectedUdpSocket(): Int {
        return try {
            val socket = DatagramSocket()
            socket.reuseAddress = true

            val protectedOk = try { protect(socket) } catch (t: Throwable) {
                ktLog("protect(UDP) 抛异常: ${t.message}")
                false
            }
            if (!protectedOk) {
                ktLog("protect(DatagramSocket) 返回 false，关闭 socket")
                try { socket.close() } catch (_: Throwable) {}
                return -1
            }

            val fd = extractFdFromDatagramSocket(socket)
            if (fd <= 0) {
                ktLog("无法从 DatagramSocket 提取 fd")
                try { socket.close() } catch (_: Throwable) {}
                return -1
            }

            protectedUdpSocket = socket
            ktLog("protected UDP socket fd=$fd")
            fd
        } catch (t: Throwable) {
            ktLog("createProtectedUdpSocket 失败: ${t.message}")
            -1
        }
    }

    private fun createProtectedStunSocket(): Int {
        return try {
            val socket = DatagramSocket()
            socket.reuseAddress = true

            val protectedOk = try { protect(socket) } catch (t: Throwable) {
                ktLog("protect(STUN) 抛异常: ${t.message}")
                false
            }
            if (!protectedOk) {
                try { socket.close() } catch (_: Throwable) {}
                return -1
            }

            val fd = extractFdFromDatagramSocket(socket)
            if (fd <= 0) {
                try { socket.close() } catch (_: Throwable) {}
                return -1
            }

            protectedStunSocket = socket
            ktLog("protected STUN socket fd=$fd")
            fd
        } catch (t: Throwable) {
            ktLog("createProtectedStunSocket 失败: ${t.message}")
            -1
        }
    }

    private fun stopVpn() {
        try { handleStop() } catch (t: Throwable) { Log.e(TAG, "stopVpn failed", t) }
    }

    private fun handleStop() {
        if (!stopping.compareAndSet(false, true)) {
            ktLog("handleStop: 已在清理中，忽略")
            return
        }
        ktLog("handleStop 开始")

        notificationActive = false
        try { handler.removeCallbacks(updateIpRunnable) } catch (_: Throwable) {}

        Thread {
            try {
                try {
                    N2nController.stop()
                } catch (t: Throwable) {
                    Log.e(TAG, "N2nController.stop failed", t)
                }
                started = false
                starting.set(false)

                try { tunInterface?.close() } catch (_: Throwable) {}
                tunInterface = null

                try {
                    protectedUdpSocket?.let {
                        if (!it.isClosed) it.close()
                    }
                } catch (_: Throwable) {}
                protectedUdpSocket = null

                try {
                    protectedStunSocket?.let {
                        if (!it.isClosed) it.close()
                    }
                } catch (_: Throwable) {}
                protectedStunSocket = null

                releaseLocks()
                ktLog("handleStop 清理完成")

                try {
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                        stopForeground(STOP_FOREGROUND_REMOVE)
                    } else {
                        @Suppress("DEPRECATION")
                        stopForeground(true)
                    }
                } catch (_: Throwable) {}
                try { stopSelf() } catch (_: Throwable) {}
            } catch (t: Throwable) {
                Log.e(TAG, "handleStop failed", t)
            } finally {
                stopping.set(false)
            }
        }.apply { name = "n2n-stop" }.start()
    }

    private fun acquireLocks() {
        try {
            val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
            wakeLock = pm.newWakeLock(
                PowerManager.PARTIAL_WAKE_LOCK, "n2n:vpn_wakelock"
            ).apply {
                setReferenceCounted(false)
                acquire()
            }
            ktLog("WakeLock acquired (PARTIAL_WAKE_LOCK)")
        } catch (t: Throwable) {
            ktLog("acquire WakeLock 失败: ${t.message}")
        }

        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager

            val lockMode = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                WifiManager.WIFI_MODE_FULL_LOW_LATENCY
            } else {
                @Suppress("DEPRECATION")
                WifiManager.WIFI_MODE_FULL_HIGH_PERF
            }

            wifiLock = wm.createWifiLock(lockMode, "n2n:vpn_wifilock").apply {
                setReferenceCounted(false)
                acquire()
            }
            ktLog("WifiLock acquired (mode=$lockMode)")
        } catch (t: Throwable) {
            ktLog("acquire WifiLock 失败: ${t.message}")
        }
    }

    private fun releaseLocks() {
        try {
            wakeLock?.let { if (it.isHeld) it.release() }
        } catch (_: Throwable) {}
        wakeLock = null

        try {
            wifiLock?.let { if (it.isHeld) it.release() }
        } catch (_: Throwable) {}
        wifiLock = null
    }

    private fun buildTunInterface(vip: String, virtualNetwork: String): ParcelFileDescriptor? {
        return try {
            val parsed = parseCidr(virtualNetwork)
                ?: run {
                    ktLog("virtualNetwork 非法: '$virtualNetwork'，回退 $DEFAULT_VIRTUAL_NETWORK")
                    parseCidr(DEFAULT_VIRTUAL_NETWORK)!!
                }
            val (networkAddr, prefixLen) = parsed

            if (!isSameSubnet(vip, networkAddr, prefixLen)) {
                ktLog("VIP $vip 不属于 $networkAddr/$prefixLen，拒绝建立 TUN")
                return null
            }

            ktLog("建立 TUN（仅组网段），绑定 IP: $vip，网段 $networkAddr/$prefixLen，MTU=$TUN_MTU")

            val builder = Builder()
                .setSession("n2n-client")
                .setMtu(TUN_MTU)
                .addAddress(vip, prefixLen)
                .addRoute(networkAddr, prefixLen)

            try { builder.addDisallowedApplication(packageName) } catch (_: Throwable) {}

            ktLog("TUN 仅接管 $networkAddr/$prefixLen，其他流量走系统默认")

            val pfd = builder.setBlocking(true).establish()
            ktLog("TUN establish 成功")
            pfd
        } catch (t: Throwable) {
            ktLog("establish TUN 失败: ${t.message}")
            null
        }
    }

    private fun parseCidr(cidr: String): Pair<String, Int>? {
        val parts = cidr.trim().split("/")
        if (parts.size != 2) return null
        val prefix = parts[1].toIntOrNull() ?: return null
        if (prefix < 0 || prefix > 32) return null

        val ipInt = ipToInt(parts[0]) ?: return null
        val mask = if (prefix == 0) 0 else (-1 shl (32 - prefix))
        val netInt = ipInt and mask

        val normalized =
            "${(netInt ushr 24) and 0xFF}.${(netInt ushr 16) and 0xFF}." +
            "${(netInt ushr 8) and 0xFF}.${netInt and 0xFF}"
        return normalized to prefix
    }

    private fun isSameSubnet(ip: String, networkAddr: String, prefixLen: Int): Boolean {
        val ipInt = ipToInt(ip) ?: return false
        val netInt = ipToInt(networkAddr) ?: return false
        val mask = if (prefixLen == 0) 0 else (-1 shl (32 - prefixLen))
        return (ipInt and mask) == (netInt and mask)
    }

    private fun ipToInt(ip: String): Int? {
        val octets = ip.trim().split(".")
        if (octets.size != 4) return null
        val nums = octets.map { it.toIntOrNull() ?: return null }
        if (nums.any { it !in 0..255 }) return null
        return (nums[0] shl 24) or (nums[1] shl 16) or (nums[2] shl 8) or nums[3]
    }

    private fun buildNotification(text: String): Notification {
        createChannelIfNeeded()
        val intent = Intent(this, MainActivity::class.java)
        val pi = PendingIntent.getActivity(
            this, 0, intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        return NotificationCompat.Builder(this, NOTIF_CHANNEL_ID)
            .setContentTitle("n2n 组网")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_sys_download_done)
            .setContentIntent(pi)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    private fun updateNotification(text: String) {
        try {
            val nm = getSystemService(NotificationManager::class.java)
            nm.notify(NOTIF_ID, buildNotification(text))
        } catch (_: Throwable) {}
    }

    private fun createChannelIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            try {
                val nm = getSystemService(NotificationManager::class.java)
                if (nm.getNotificationChannel(NOTIF_CHANNEL_ID) == null) {
                    val ch = NotificationChannel(
                        NOTIF_CHANNEL_ID, "n2n VPN",
                        NotificationManager.IMPORTANCE_LOW
                    ).apply { description = "n2n 组网运行状态" }
                    nm.createNotificationChannel(ch)
                }
            } catch (_: Throwable) {}
        }
    }

    override fun onDestroy() {
        ktLog("onDestroy")
        destroyed = true
        notificationActive = false

        try { unregisterReceiver(tzReceiver) } catch (_: Throwable) {}

        unregisterNetworkCallback()

        try { handler.removeCallbacksAndMessages(null) } catch (_: Throwable) {}
        handleStop()
        try { super.onDestroy() } catch (_: Throwable) {}
    }
}
