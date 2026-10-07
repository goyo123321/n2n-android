package com.n2n.android

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
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
        const val EXTRA_SHARE_DIR = "share_dir"
        const val EXTRA_PREFERRED_IP = "preferred_ip"
    }

    private var tunInterface: ParcelFileDescriptor? = null
    @Volatile private var started = false
    private val starting = AtomicBoolean(false)   // ★ 防止重复启动（TOCTOU）
    private val stopping = AtomicBoolean(false)   // ★ 防止重复清理
    private val handler = Handler(Looper.getMainLooper())

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null
    private var protectedUdpSocket: DatagramSocket? = null
    private var protectedStunSocket: DatagramSocket? = null

    // Protector 是 gomobile 生成的 Kotlin interface，无构造函数
    inner class ServiceProtector : Protector {
        override fun protect(fd: Long): Boolean {
            return try {
                this@N2nVpnService.protect(fd.toInt())
            } catch (e: Exception) {
                Log.w(TAG, "protect($fd) 失败: ${e.message}")
                false
            }
        }
    }

    private fun ktLog(msg: String) {
        val ts = SimpleDateFormat("HH:mm:ss", Locale.US).format(Date())
        val line = "[$ts] [K] $msg"

        // ★ 优先用 filesDir（支持多用户 / work profile），其余作为 fallback
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
            } catch (_: Exception) {}
        }
        if (!written) Log.e(TAG, "ktLog 全部路径写入失败")
        Log.i(TAG, msg)
    }

    private val updateIpRunnable = object : Runnable {
        private var attempts = 0
        override fun run() {
            attempts++
            val ip = N2nController.getVirtualIP()
            if (ip.isNotEmpty()) {
                updateNotification("已连接 · 虚拟 IP $ip")
                Log.i(TAG, "VPN 虚拟 IP: $ip")
            } else if (attempts < 30) {
                updateNotification("正在连接... (${attempts}s)")
                handler.postDelayed(this, 1000)
            } else {
                // ★ 超时后降频继续轮询，不再直接放弃
                updateNotification("正在连接... (${attempts}s)")
                handler.postDelayed(this, 3000)
            }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        ktLog("onStartCommand action=${intent?.action}")
        when (intent?.action) {
            ACTION_START -> handleStart(intent)
            ACTION_STOP -> handleStop()
            else -> {
                ktLog("unknown action or null intent, stopSelf")
                stopSelf()
                return START_NOT_STICKY
            }
        }
        return START_NOT_STICKY
    }

    // ★ VPN 权限被系统或另一个 VPN 抢占时回调
    override fun onRevoke() {
        ktLog("onRevoke: VPN 权限被撤销")
        handleStop()
        super.onRevoke()
    }

    private fun handleStart(intent: Intent) {
        if (started) {
            ktLog("handleStart: 已启动，跳过")
            return
        }
        // ★ CAS：防止双击导致两次启动流程并行
        if (!starting.compareAndSet(false, true)) {
            ktLog("handleStart: 已有启动流程在跑，忽略")
            return
        }

        ktLog("handleStart 开始")

        val signalingUrl = intent.getStringExtra(EXTRA_SIGNALING_URL) ?: run {
            ktLog("handleStart: 无 signalingUrl，退出")
            starting.set(false)
            stopSelf()
            return
        }
        val preferredIp = intent.getStringExtra(EXTRA_PREFERRED_IP) ?: ""
        val roomId = intent.getStringExtra(EXTRA_ROOM_ID) ?: "default-room"
        val clientId = intent.getStringExtra(EXTRA_CLIENT_ID) ?: ""
        val nodeName = intent.getStringExtra(EXTRA_NODE_NAME) ?: "Android"
        val connectToken = intent.getStringExtra(EXTRA_CONNECT_TOKEN) ?: ""
        val shareDir = intent.getStringExtra(EXTRA_SHARE_DIR)
            ?: ShareDirManager.getDefaultShareDir(this).absolutePath
        ktLog("参数读取完成 room=$roomId")

        try {
            startForeground(NOTIF_ID, buildNotification("正在获取虚拟 IP..."))
            acquireLocks()
            ktLog("startForeground + acquireLocks 完成")
        } catch (t: Throwable) {
            ktLog("startForeground 崩溃: ${t.message}")
            Log.e(TAG, "startForeground failed", t)
            starting.set(false)
            stopVpn()
            return
        }

        val config = Config().apply {
            setSignalingURL(signalingUrl)
            setRoomID(roomId)
            setClientID(clientId)
            setNodeName(nodeName)
            setConnectToken(connectToken)
            setShareDir(shareDir)
            setPreferredIP(preferredIp)
            setPreferredPort(443)
        }
        ktLog("Config 构造完成")

        Thread {
            try {
                ktLog("开始 FetchVIP")
                val tmpClient = Client()
                val vip = tmpClient.fetchVirtualIP(config)
                ktLog("FetchVIP 返回: '$vip'")

                handler.post {
                    try {
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

                        ktLog("创建 protected STUN socket")
                        val stunFd = createProtectedStunSocket()
                        ktLog("STUN fd=$stunFd")

                        // ★ UDP 是 P2P 打洞的必要条件。fd 无效时直接失败，
                        //   避免 Go 侧回退到未 protect 的默认 socket（会被 TUN 捕获 → 死循环）
                        if (udpFd <= 0) {
                            ktLog("UDP fd 无效，无法建立 P2P，退出")
                            updateNotification("UDP socket 创建失败")
                            handler.postDelayed({ stopVpn() }, 3000)
                            return@post
                        }

                        ktLog("调用 startAsync")
                        N2nController.startAsync(
                            tunFd, udpFd, stunFd, config, ServiceProtector()
                        ) { err ->
                            handler.post {
                                if (err.isNotEmpty()) {
                                    ktLog("startAsync 失败: $err")
                                    updateNotification("启动失败: $err")
                                    handler.postDelayed({ stopVpn() }, 3000)
                                } else {
                                    ktLog("startAsync 成功")
                                    started = true
                                    starting.set(false)   // ★ 成功才清 starting
                                    handler.post(updateIpRunnable)
                                }
                            }
                        }
                    } catch (t: Throwable) {
                        ktLog("handler.post 崩溃: ${t.message}")
                        Log.e(TAG, "handler.post failed", t)
                        updateNotification("启动失败: ${t.message}")
                        handler.postDelayed({ stopVpn() }, 3000)
                    }
                }
            } catch (t: Throwable) {
                ktLog("handleStart Thread 崩溃: ${t.message}")
                Log.e(TAG, "handleStart thread failed", t)
                handler.post {
                    updateNotification("启动失败: ${t.message}")
                    handler.postDelayed({ stopVpn() }, 3000)
                }
            }
        }.start()
    }

    private fun createProtectedUdpSocket(): Int {
        return try {
            val socket = DatagramSocket()
            socket.reuseAddress = true
            val ok = protect(socket)
            if (!ok) ktLog("protect(DatagramSocket) 返回 false")
            protectedUdpSocket = socket
            val pfd = ParcelFileDescriptor.fromDatagramSocket(socket)
            val fd = pfd.detachFd()
            ktLog("protected UDP socket fd=$fd")
            fd
        } catch (e: Exception) {
            ktLog("createProtectedUdpSocket 失败: ${e.message}")
            -1
        }
    }

    private fun createProtectedStunSocket(): Int {
        return try {
            val socket = DatagramSocket()
            socket.reuseAddress = true
            protect(socket)
            protectedStunSocket = socket
            val pfd = ParcelFileDescriptor.fromDatagramSocket(socket)
            val fd = pfd.detachFd()
            ktLog("protected STUN socket fd=$fd")
            fd
        } catch (e: Exception) {
            ktLog("createProtectedStunSocket 失败: ${e.message}")
            -1
        }
    }

    private fun stopVpn() { handleStop() }

    private fun handleStop() {
        // ★ 保证同一时刻只有一次清理流程
        if (!stopping.compareAndSet(false, true)) {
            ktLog("handleStop: 已在清理中，忽略")
            return
        }
        try {
            ktLog("handleStop 开始")
            handler.removeCallbacks(updateIpRunnable)

            if (started) {
                N2nController.stop()
                started = false
            }
            starting.set(false)

            try { tunInterface?.close() } catch (_: Exception) {}
            tunInterface = null
            try { protectedUdpSocket?.close() } catch (_: Exception) {}
            protectedUdpSocket = null
            try { protectedStunSocket?.close() } catch (_: Exception) {}
            protectedStunSocket = null
            releaseLocks()
            ktLog("handleStop 清理完成")

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                stopForeground(STOP_FOREGROUND_REMOVE)
            } else {
                @Suppress("DEPRECATION")
                stopForeground(true)
            }
            stopSelf()
        } finally {
            stopping.set(false)
        }
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
        } catch (e: Exception) { ktLog("acquire WakeLock 失败: ${e.message}") }
        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            wifiLock = wm.createWifiLock(
                WifiManager.WIFI_MODE_FULL_HIGH_PERF, "n2n:vpn_wifilock"
            ).apply {
                setReferenceCounted(false)
                acquire()
            }
        } catch (e: Exception) { ktLog("acquire WifiLock 失败: ${e.message}") }
    }

    private fun releaseLocks() {
        try { wakeLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) {}
        wakeLock = null
        try { wifiLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) {}
        wifiLock = null
    }

    // ============================================================
    // TUN：接管 0.0.0.0/0 + 下发 DNS 指向虚拟 IP
    // 完全靠 socket protect 保证信令 / TURN / WSOut 不绕 TUN
    // ============================================================
    private fun buildTunInterface(vip: String): ParcelFileDescriptor? {
        return try {
            ktLog("建立 TUN（全流量 + protect socket + 双 DNS），绑定 IP: $vip")

            val builder = Builder()
                .setSession("n2n-client")
                .setMtu(1280)
                .addAddress(vip, 24)
                .addRoute("10.64.0.0", 24)
                .addRoute("0.0.0.0", 0)
                .addDnsServer(vip)   // 系统 DNS 指向虚拟 IP → netstack 处理

            ktLog("TUN 接管 0.0.0.0/0，DNS 指向 $vip，信令/TURN/WSOut 靠 socket protect 走物理网络")

            val pfd = builder.setBlocking(true).establish()
            ktLog("TUN establish 成功")
            pfd
        } catch (e: Exception) {
            ktLog("establish TUN 失败: ${e.message}")
            null
        }
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
        } catch (_: Exception) {}
    }

    private fun createChannelIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val nm = getSystemService(NotificationManager::class.java)
            if (nm.getNotificationChannel(NOTIF_CHANNEL_ID) == null) {
                val ch = NotificationChannel(
                    NOTIF_CHANNEL_ID, "n2n VPN",
                    NotificationManager.IMPORTANCE_LOW
                ).apply { description = "n2n 组网运行状态" }
                nm.createNotificationChannel(ch)
            }
        }
    }

    override fun onDestroy() {
        ktLog("onDestroy")
        handler.removeCallbacks(updateIpRunnable)
        handleStop()
        super.onDestroy()
    }
}
