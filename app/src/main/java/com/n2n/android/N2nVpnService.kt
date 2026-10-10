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
import java.io.FileDescriptor
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
    }

    private var tunInterface: ParcelFileDescriptor? = null
    @Volatile private var started = false
    private val starting = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private val handler = Handler(Looper.getMainLooper())

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null

    private var protectedUdpSocket: DatagramSocket? = null
    private var protectedStunSocket: DatagramSocket? = null
    private val udpFdHandedOff = AtomicBoolean(false)
    private val stunFdHandedOff = AtomicBoolean(false)

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
        } catch (_: Throwable) {
        }
        Log.i(TAG, msg)
    }

    private val updateIpRunnable = object : Runnable {
        private var attempts = 0
        override fun run() {
            try {
                attempts++
                val ip = N2nController.getVirtualIP()
                if (ip.isNotEmpty()) {
                    updateNotification("已连接 · 虚拟 IP $ip")
                    Log.i(TAG, "VPN 虚拟 IP: $ip")
                } else if (attempts < 30) {
                    updateNotification("正在连接... (${attempts}s)")
                    handler.postDelayed(this, 1000)
                } else {
                    updateNotification("正在连接... (${attempts}s)")
                    handler.postDelayed(this, 3000)
                }
            } catch (t: Throwable) {
                Log.e(TAG, "updateIpRunnable failed", t)
            }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        ktLog("onStartCommand action=${intent?.action}")
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

    override fun onRevoke() {
        ktLog("onRevoke: VPN 权限被撤销")
        try { handleStop() } catch (_: Throwable) {}
        try { super.onRevoke() } catch (_: Throwable) {}
    }

    private fun handleStart(intent: Intent) {
        if (started) {
            ktLog("handleStart: 已启动，跳过")
            return
        }
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

        ktLog("参数读取完成 room=$roomId token=${if (connectToken.isEmpty()) "<empty>" else "***"}")

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
            Log.e(TAG, "Config build failed", t)
            starting.set(false)
            stopVpn()
            return
        }
        ktLog("Config 构造完成")

        Thread {
            try {
                ktLog("开始 FetchVIP")
                val tmpClient = Client()
                val vip = tmpClient.fetchVirtualIP(config) ?: ""
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
                        if (udpFd <= 0) {
                            ktLog("UDP fd 无效，退出")
                            updateNotification("UDP socket 创建失败")
                            handler.postDelayed({ stopVpn() }, 3000)
                            return@post
                        }

                        ktLog("创建 protected STUN socket")
                        val stunFd = createProtectedStunSocket()
                        ktLog("STUN fd=$stunFd")

                        udpFdHandedOff.set(true)
                        if (stunFd > 0) stunFdHandedOff.set(true)

                        ktLog("调用 startAsync")
                        N2nController.startAsync(
                            tunFd, udpFd, stunFd, config, ServiceProtector()
                        ) { err ->
                            handler.post {
                                try {
                                    if (err.isNotEmpty()) {
                                        ktLog("startAsync 失败: $err")
                                        updateNotification("启动失败: $err")
                                        handler.postDelayed({ stopVpn() }, 3000)
                                    } else {
                                        ktLog("startAsync 成功")
                                        started = true
                                        starting.set(false)
                                        handler.post(updateIpRunnable)
                                    }
                                } catch (t: Throwable) {
                                    Log.e(TAG, "startAsync callback failed", t)
                                }
                            }
                        }
                    } catch (t: Throwable) {
                        ktLog("handler.post 崩溃: ${t.message}")
                        Log.e(TAG, "handler.post failed", t)
                        try { updateNotification("启动失败: ${t.message}") } catch (_: Throwable) {}
                        handler.postDelayed({ stopVpn() }, 3000)
                    }
                }
            } catch (t: Throwable) {
                ktLog("handleStart Thread 崩溃: ${t.message}")
                Log.e(TAG, "handleStart thread failed", t)
                handler.post {
                    try { updateNotification("启动失败: ${t.message}") } catch (_: Throwable) {}
                    handler.postDelayed({ stopVpn() }, 3000)
                }
            }
        }.start()
    }

    // ============================================================
    // 从 DatagramSocket 提取 fd
    // ============================================================
    private fun extractFdFromDatagramSocket(socket: DatagramSocket): Int {
        try {
            val m = ParcelFileDescriptor::class.java.getDeclaredMethod(
                "fromDatagramSocket", DatagramSocket::class.java
            )
            m.isAccessible = true
            val pfd = m.invoke(null, socket) as? ParcelFileDescriptor
            if (pfd != null) {
                val fd = extractDescriptorFromPfd(pfd)
                if (fd > 0) return fd
            }
        } catch (_: Throwable) {}

        try {
            val implField = DatagramSocket::class.java.getDeclaredField("impl")
            implField.isAccessible = true
            val impl = implField.get(socket) ?: return -1

            var cls: Class<*>? = impl.javaClass
            var fdField: java.lang.reflect.Field? = null
            while (cls != null && cls != Any::class.java) {
                try {
                    fdField = cls.getDeclaredField("fd")
                    break
                } catch (_: NoSuchFieldException) {
                    cls = cls.superclass
                }
            }
            if (fdField == null) return -1
            fdField.isAccessible = true
            val fdObj = fdField.get(impl) as? FileDescriptor ?: return -1

            val descriptorField = FileDescriptor::class.java
                .getDeclaredField("descriptor")
            descriptorField.isAccessible = true
            return descriptorField.getInt(fdObj)
        } catch (_: Throwable) {}

        return -1
    }

    private fun extractDescriptorFromPfd(pfd: ParcelFileDescriptor): Int {
        return try {
            val mFdField = ParcelFileDescriptor::class.java.getDeclaredField("mFd")
            mFdField.isAccessible = true
            val fileDescriptor = mFdField.get(pfd) as? FileDescriptor ?: return -1
            val descriptorField = FileDescriptor::class.java
                .getDeclaredField("descriptor")
            descriptorField.isAccessible = true
            descriptorField.getInt(fileDescriptor)
        } catch (_: Throwable) {
            -1
        }
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
        try {
            ktLog("handleStop 开始")
            handler.removeCallbacks(updateIpRunnable)

            if (started) {
                try { N2nController.stop() } catch (t: Throwable) {
                    Log.e(TAG, "N2nController.stop failed", t)
                }
                started = false
            }
            starting.set(false)

            try { tunInterface?.close() } catch (_: Throwable) {}
            tunInterface = null

            try { protectedUdpSocket?.close() } catch (_: Throwable) {}
            protectedUdpSocket = null
            udpFdHandedOff.set(false)

            try { protectedStunSocket?.close() } catch (_: Throwable) {}
            protectedStunSocket = null
            stunFdHandedOff.set(false)

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
            Log.e(TAG, "handleStop 异常", t)
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
        try { handler.removeCallbacks(updateIpRunnable) } catch (_: Throwable) {}
        handleStop()
        try { super.onDestroy() } catch (_: Throwable) {}
    }
}
