package com.n2n.android

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.VpnService
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import android.util.Log
import androidx.core.app.NotificationCompat
import com.n2n.mobile.Client
import com.n2n.mobile.Config
import com.n2n.mobile.Protector
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.net.DatagramSocket
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

class N2nVpnService : VpnService() {

    companion object {
        private const val TAG = "N2nVpnService"
        private const val NOTIF_CHANNEL_ID = "n2n_vpn"
        private const val NOTIF_ID = 1001

        const val ACTION_START = "com.n2n.android.START"
        const val ACTION_STOP = "com.n2n.android.STOP"

        const val EXTRA_SIGNALING_URL = "signaling_url"
        const val EXTRA_ROOM_ID = "room_id"
        const val EXTRA_CLIENT_ID = "client_id"
        const val EXTRA_NODE_NAME = "node_name"
        const val EXTRA_CONNECT_TOKEN = "connect_token"
        const val EXTRA_PREFERRED_IP = "preferred_ip"

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

    // ★ 仅作为 Kotlin 侧引用。fd 所有权归 Go（detachFd 已转移）
    private var protectedUdpSocket: DatagramSocket? = null
    private var protectedStunSocket: DatagramSocket? = null

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

    // ============================================================
    // 崩溃捕获
    // ============================================================

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

    // ============================================================
    // 日志
    // ============================================================

    private fun ktLog(msg: String) {
        try {
            val ts = SimpleDateFormat("HH:mm:ss", Locale.US).format(Date())
            val line = "[$ts] [K] $msg"

            val candidates = mutableListOf<File>()
            try { candidates.add(File(filesDir, "n2n.log")) } catch (_: Throwable) {}
            candidates.add(File("/data/user/0/com.n2n.android/files/n2n.log"))
            candidates.add(File("/data/data/com.n2n.android/files/n2n.log"))

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

    // ============================================================
    // 通知刷新
    // ============================================================

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

    // ============================================================
    // 生命周期
    // ============================================================

    override fun onCreate() {
        super.onCreate()
        installCrashHandler()
        ktLog("onCreate: 进程启动/Service 创建")
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        ktLog("onStartCommand action=${intent?.action} started=$started")

        // ★ Android 12+ 5 秒内必须 startForeground
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

    private fun handleStart(intent: Intent) {
        if (started) {
            ktLog("handleStart: 已启动，跳过")
            updateNotification("已连接")
            return
        }

        // Service 新实例但 Controller 还在跑 → 强制停旧的
        if (N2nController.isRunning()) {
            ktLog("handleStart: Controller 仍在 running，先强制停止")
            try { N2nController.stop() } catch (t: Throwable) {
                Log.e(TAG, "force stop failed", t)
            }
            try { Thread.sleep(300) } catch (_: InterruptedException) {}
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
        val clientId = intent.getStringExtra(EXTRA_CLIENT_ID) ?: ""
        val nodeName = intent.getStringExtra(EXTRA_NODE_NAME) ?: "Android"
        val connectToken = intent.getStringExtra(EXTRA_CONNECT_TOKEN) ?: ""

        ktLog("参数读取完成 room=$roomId token=${if (connectToken.isEmpty()) "<empty>" else "***"}")

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
            var fetchOk = false
            var tmpClient: Client? = null
            try {
                ktLog("开始 FetchVIP")
                tmpClient = Client()
                vip = tmpClient.fetchVirtualIP(config) ?: ""
                fetchOk = true
                ktLog("FetchVIP 返回: '$vip'")
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
                    val pfd = buildTunInterface(vip)
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

    // ============================================================
    // 从 DatagramSocket 提取 fd —— 必须 detachFd
    // ============================================================
    //
    // ParcelFileDescriptor.fromDatagramSocket(socket) 会 dup socket 的 fd，
    // 返回持有 dup fd 的新 PFD。PFD 被 GC 时 finalizer 会 close 这个 fd。
    //
    // 若只反射读 mFd.descriptor 而不 detach：
    //   - PFD 仍持有 dup fd
    //   - GC → finalizer close(dup fd)
    //   - fd 号被系统回收 → 分配给新 socket（WS / TURN 等）
    //   - Go 继续按旧 fd 号读写 → 数据错乱或误关无关 socket
    //
    // detachFd() 把 fd 从 PFD 摘出，PFD 的 mFd 置 null，
    // finalizer 不再 close 它。fd 所有权转移到调用方（Go）。
    //
    private fun extractFdFromDatagramSocket(socket: DatagramSocket): Int {
        // 路径 1：API 30+ 公开 API
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            try {
                val pfd = ParcelFileDescriptor.fromDatagramSocket(socket)
                if (pfd != null) {
                    val fd = pfd.detachFd()
                    if (fd > 0) return fd
                }
            } catch (_: Throwable) {}
        }

        // 路径 2：反射（Android 10 及以下）
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

        // 不走 DatagramSocket.impl.fd 反射路径：
        //   impl.fd 是 socket 自己的 fd，反射不改所有权，
        //   socket finalizer 仍会 close 它。
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

    // ============================================================
    // handleStop
    // ============================================================

    private fun handleStop() {
        if (!stopping.compareAndSet(false, true)) {
            ktLog("handleStop: 已在清理中，忽略")
            return
        }
        ktLog("handleStop 开始")

        // 先停通知刷新
        notificationActive = false
        try { handler.removeCallbacks(updateIpRunnable) } catch (_: Throwable) {}

        Thread {
            try {
                // ★ 无条件调 N2nController.stop()
                //   启动进行中时 started 还是 false，但 running 可能已 true，
                //   必须通知 Controller 取消，否则 Client 泄漏
                try {
                    N2nController.stop()
                } catch (t: Throwable) {
                    Log.e(TAG, "N2nController.stop failed", t)
                }
                started = false
                starting.set(false)

                // TUN PFD：Go 已 detachFd，PFD 的 mFd 为 null，
                // close() 是 no-op
                try { tunInterface?.close() } catch (_: Throwable) {}
                tunInterface = null

                // ★ 不 close protectedUdpSocket / protectedStunSocket：
                //   底层 fd 的所有权已通过 detachFd 转移给 Go，
                //   Go 在 Edge.Stop() 里 close 它。
                //   Kotlin 侧仅置空引用（socket 对象及其原 fd 会被 GC 兜底）。
                protectedUdpSocket = null
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

    // ============================================================
    // 锁
    // ============================================================

    private fun acquireLocks() {
        try {
            val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
            wakeLock = pm.newWakeLock(
                PowerManager.PARTIAL_WAKE_LOCK, "n2n:vpn_wakelock"
            ).apply {
                setReferenceCounted(false)
                acquire()
            }
        } catch (t: Throwable) { ktLog("acquire WakeLock 失败: ${t.message}") }
        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            wifiLock = wm.createWifiLock(
                WifiManager.WIFI_MODE_FULL_HIGH_PERF, "n2n:vpn_wifilock"
            ).apply {
                setReferenceCounted(false)
                acquire()
            }
        } catch (t: Throwable) { ktLog("acquire WifiLock 失败: ${t.message}") }
    }

    private fun releaseLocks() {
        try { wakeLock?.let { if (it.isHeld) it.release() } } catch (_: Throwable) {}
        wakeLock = null
        try { wifiLock?.let { if (it.isHeld) it.release() } } catch (_: Throwable) {}
        wifiLock = null
    }

    // ============================================================
    // TUN
    // ============================================================

    private fun buildTunInterface(vip: String): ParcelFileDescriptor? {
        return try {
            ktLog("建立 TUN（仅组网段），绑定 IP: $vip")

            val builder = Builder()
                .setSession("n2n-client")
                .setMtu(1280)
                .addAddress(vip, 24)
                .addRoute("10.64.0.0", 24)

            try { builder.addDisallowedApplication(packageName) } catch (_: Throwable) {}

            ktLog("TUN 仅接管 10.64.0.0/24，其他流量走系统默认")

            val pfd = builder.setBlocking(true).establish()
            ktLog("TUN establish 成功")
            pfd
        } catch (t: Throwable) {
            ktLog("establish TUN 失败: ${t.message}")
            null
        }
    }

    // ============================================================
    // 通知
    // ============================================================

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
        try { handler.removeCallbacksAndMessages(null) } catch (_: Throwable) {}
        handleStop()
        try { super.onDestroy() } catch (_: Throwable) {}
    }
}
