package com.n2n.android

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.net.IpPrefix
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
import java.net.DatagramSocket
import java.net.HttpURLConnection
import java.net.Inet6Address
import java.net.InetAddress
import java.net.URL
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import org.json.JSONObject

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
    private var started = false
    private val handler = Handler(Looper.getMainLooper())

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null
    private var protectedUdpSocket: DatagramSocket? = null
    private var protectedStunSocket: DatagramSocket? = null

    // ★ Kotlin 日志直接 append 到 Go 用的同一个文件
    //   App 重开时 Go init() 读回该文件 → 日志页能显示 Kotlin 日志
    private fun ktLog(msg: String) {
        try {
            val f = java.io.File(filesDir, "n2n.log")
            val ts = SimpleDateFormat("HH:mm:ss", Locale.US).format(Date())
            f.appendText("[$ts] [K] $msg\n")
        } catch (_: Exception) {}
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
                updateNotification("连接超时")
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

    private fun handleStart(intent: Intent) {
        if (started) {
            ktLog("handleStart: 已启动，跳过")
            return
        }
        ktLog("handleStart 开始")

        val signalingUrl = intent.getStringExtra(EXTRA_SIGNALING_URL) ?: run {
            ktLog("handleStart: 无 signalingUrl，退出")
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

                ktLog("开始查 TURN 凭证")
                val turnIPs = fetchTURNServerIPs(signalingUrl, connectToken)
                ktLog("TURN IP 数量: ${turnIPs.size}")

                val preferredIPs = resolvePreferredIPs(preferredIp)
                ktLog("优选 IP 数量: ${preferredIPs.size}")

                handler.post {
                    try {
                        if (vip.isEmpty()) {
                            ktLog("VIP 为空，退出")
                            updateNotification("获取虚拟 IP 失败")
                            handler.postDelayed({ stopVpn() }, 3000)
                            return@post
                        }

                        ktLog("开始建立 TUN")
                        val pfd = buildTunInterface(vip, turnIPs, preferredIPs)
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

                        ktLog("调用 startAsync")
                        N2nController.startAsync(tunFd, udpFd, stunFd, config) { err ->
                            handler.post {
                                if (err.isNotEmpty()) {
                                    ktLog("startAsync 失败: $err")
                                    updateNotification("启动失败: $err")
                                    handler.postDelayed({ stopVpn() }, 3000)
                                } else {
                                    ktLog("startAsync 成功")
                                    started = true
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

    private fun resolvePreferredIPs(preferredIP: String): List<String> {
        if (preferredIP.isBlank()) return emptyList()

        var s = preferredIP.trim()

        if (s.startsWith("[")) {
            val end = s.indexOf(']')
            if (end > 0) {
                s = s.substring(1, end)
            }
        } else if (s.count { it == ':' } == 1) {
            val colon = s.lastIndexOf(':')
            val portStr = s.substring(colon + 1)
            if (portStr.isNotEmpty() && portStr.all { it.isDigit() }) {
                s = s.substring(0, colon)
            }
        }

        if (s.isEmpty()) return emptyList()

        val ips = mutableListOf<String>()

        if (isIPv4(s) || isIPv6(s)) {
            ips.add(s)
            ktLog("优选 IP: $s")
            return ips
        }

        try {
            val addresses = InetAddress.getAllByName(s)
            for (addr in addresses) {
                val ip = addr.hostAddress ?: continue
                if (!ips.contains(ip)) {
                    ips.add(ip)
                    ktLog("优选 IP: $s → $ip")
                }
            }
        } catch (e: Exception) {
            ktLog("解析优选 IP $s 失败: ${e.message}")
        }
        return ips
    }

    private fun isIPv4(s: String): Boolean {
        val parts = s.split(".")
        if (parts.size != 4) return false
        return parts.all { p ->
            val n = p.toIntOrNull() ?: return false
            n in 0..255
        }
    }

    private fun isIPv6(s: String): Boolean {
        if (!s.contains(":")) return false
        return try {
            InetAddress.getByName(s) is Inet6Address
        } catch (e: Exception) {
            false
        }
    }

    private fun fetchTURNServerIPs(signalingUrl: String, connectToken: String): List<String> {
        return try {
            var httpBase = signalingUrl
            if (httpBase.startsWith("wss://")) {
                httpBase = "https://" + httpBase.removePrefix("wss://")
            } else if (httpBase.startsWith("ws://")) {
                httpBase = "http://" + httpBase.removePrefix("ws://")
            }
            httpBase = httpBase.trimEnd('/')

            var credURL = "$httpBase/api/turn-credentials?ttl=60"
            if (connectToken.isNotEmpty()) {
                credURL += "&token=" + URLEncoder.encode(connectToken, "UTF-8")
            }

            ktLog("查询 TURN 凭证: $credURL")

            val conn = URL(credURL).openConnection() as HttpURLConnection
            conn.connectTimeout = 5000
            conn.readTimeout = 5000
            conn.requestMethod = "GET"

            if (conn.responseCode != 200) {
                ktLog("TURN 凭证查询失败: HTTP ${conn.responseCode}")
                return emptyList()
            }

            val body = conn.inputStream.bufferedReader().use { it.readText() }
            val json = JSONObject(body)

            if (!json.optBoolean("success", false)) {
                ktLog("TURN 未配置: ${json.optString("error")}")
                return emptyList()
            }

            val servers = json.optJSONArray("servers") ?: return emptyList()
            val ips = mutableListOf<String>()

            for (i in 0 until servers.length()) {
                val srv = servers.getJSONObject(i)
                val url = srv.optString("url", "")
                val host = parseTurnHost(url)
                if (host.isEmpty()) continue

                if (isIPv4(host)) {
                    if (!ips.contains(host)) {
                        ips.add(host)
                        ktLog("  TURN: $url → $host")
                    }
                    continue
                }

                try {
                    val addresses = InetAddress.getAllByName(host)
                    for (addr in addresses) {
                        val ip = addr.hostAddress ?: continue
                        if (isIPv4(ip) && !ips.contains(ip)) {
                            ips.add(ip)
                            ktLog("  TURN: $url → $host → $ip")
                        }
                    }
                } catch (e: Exception) {
                    ktLog("解析 TURN 域名 $host 失败: ${e.message}")
                }
            }
            ips
        } catch (e: Exception) {
            ktLog("fetchTURNServerIPs 异常: ${e.message}")
            emptyList()
        }
    }

    private fun parseTurnHost(url: String): String {
        var s = url
        s = s.removePrefix("turn://").removePrefix("turns://")
            .removePrefix("turn:").removePrefix("turns:")
        s = s.removePrefix("//")
        val q = s.indexOf('?')
        if (q >= 0) s = s.substring(0, q)
        val colon = s.lastIndexOf(':')
        return if (colon > 0) s.substring(0, colon) else s
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
        ktLog("handleStop 开始")
        handler.removeCallbacks(updateIpRunnable)
        if (started) {
            N2nController.stop()
            started = false
        }
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

    private fun buildTunInterface(
        vip: String,
        turnIPs: List<String>,
        preferredIPs: List<String>
    ): ParcelFileDescriptor? {
        return try {
            ktLog("建立 TUN（全流量），绑定 IP: $vip")
            val builder = Builder()
                .setSession("n2n-client")
                .setMtu(1280)
                .addAddress(vip, 24)
                .addRoute("10.64.0.0", 24)

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                builder.addRoute("0.0.0.0", 0)

                val cfSegments = listOf(
                    "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
                    "104.16.0.0/13", "104.24.0.0/14", "108.162.192.0/18",
                    "131.0.72.0/22", "141.101.64.0/18", "162.158.0.0/15",
                    "172.64.0.0/13", "173.245.48.0/20", "188.114.96.0/20",
                    "190.93.240.0/20", "197.234.240.0/22", "198.41.128.0/17"
                )
                for (seg in cfSegments) {
                    excludeIpPrefix(builder, seg)
                }

                for (ip in turnIPs) {
                    excludeIpPrefix(builder, "$ip/32", "TURN")
                }

                for (ip in preferredIPs) {
                    if (isIPv4(ip)) {
                        excludeIpPrefix(builder, "$ip/32", "preferred")
                    } else {
                        ktLog("优选 IP (IPv6, 无需排除): $ip")
                    }
                }
            }

            val pfd = builder.setBlocking(true).establish()
            ktLog("TUN establish 成功")
            pfd
        } catch (e: Exception) {
            ktLog("establish TUN 失败: ${e.message}")
            null
        }
    }

    private fun excludeIpPrefix(builder: Builder, cidr: String, label: String = "") {
        try {
            val slashIdx = cidr.indexOf('/')
            if (slashIdx < 0) {
                ktLog("excludeRoute $cidr 失败: 无 /")
                return
            }
            val ipStr = cidr.substring(0, slashIdx)
            val prefixLen = cidr.substring(slashIdx + 1).toInt()
            val addr = InetAddress.getByName(ipStr)
            val prefix = IpPrefix(addr, prefixLen)
            builder.excludeRoute(prefix)
            val tag = if (label.isEmpty()) "" else "[$label] "
            ktLog("  + excludeRoute: $tag$cidr")
        } catch (e: Exception) {
            ktLog("excludeRoute $cidr 失败: ${e.message}")
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
