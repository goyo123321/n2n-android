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
import java.net.DatagramSocket
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.URL
import java.net.URLEncoder
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
        when (intent?.action) {
            ACTION_START -> handleStart(intent)
            ACTION_STOP -> handleStop()
            else -> Log.w(TAG, "unknown action: ${intent?.action}")
        }
        return START_STICKY
    }

    private fun handleStart(intent: Intent) {
        if (started) return

        val signalingUrl = intent.getStringExtra(EXTRA_SIGNALING_URL) ?: run {
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

        startForeground(NOTIF_ID, buildNotification("正在获取虚拟 IP..."))
        acquireLocks()

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

        Thread {
            val tmpClient = Client()
            val vip = tmpClient.fetchVirtualIP(config)

            val turnIPs = fetchTURNServerIPs(signalingUrl, connectToken)
            Log.i(TAG, "TURN 服务器 IP 数量: ${turnIPs.size}")

            val preferredIPs = resolvePreferredIPs(preferredIp)
            Log.i(TAG, "优选 IP 数量: ${preferredIPs.size}")

            handler.post {
                if (vip.isEmpty()) {
                    updateNotification("获取虚拟 IP 失败")
                    handler.postDelayed({ stopVpn() }, 3000)
                    return@post
                }
                val pfd = buildTunInterface(vip, turnIPs, preferredIPs)
                if (pfd == null) {
                    updateNotification("建立 TUN 失败")
                    handler.postDelayed({ stopVpn() }, 3000)
                    return@post
                }
                tunInterface = pfd
                val tunFd = pfd.detachFd()

                val udpFd = createProtectedUdpSocket()
                val stunFd = createProtectedStunSocket()

                N2nController.startAsync(tunFd, udpFd, stunFd, config) { err ->
                    handler.post {
                        if (err.isNotEmpty()) {
                            Log.e(TAG, "client start failed: $err")
                            updateNotification("启动失败: $err")
                            handler.postDelayed({ stopVpn() }, 3000)
                        } else {
                            started = true
                            handler.post(updateIpRunnable)
                        }
                    }
                }
            }
        }.start()
    }

    // ============================================================
    // 优选 IP 解析
    // ============================================================

    private fun resolvePreferredIPs(preferredIP: String): List<String> {
        if (preferredIP.isBlank()) return emptyList()

        var s = preferredIP.trim()
        val colon = s.lastIndexOf(':')
        if (colon > 0 && s.substring(colon + 1).all { it.isDigit() }) {
            s = s.substring(0, colon)
        }
        if (s.isEmpty()) return emptyList()

        val ips = mutableListOf<String>()

        if (isIPv4(s)) {
            ips.add(s)
            Log.i(TAG, "优选 IP: $s")
            return ips
        }

        try {
            val addresses = InetAddress.getAllByName(s)
            for (addr in addresses) {
                val ip = addr.hostAddress ?: continue
                if (isIPv4(ip) && !ips.contains(ip)) {
                    ips.add(ip)
                    Log.i(TAG, "优选 IP: $s → $ip")
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "解析优选 IP $s 失败: ${e.message}")
        }
        return ips
    }

    // ============================================================
    // TURN 服务器 IP 查询
    // ============================================================

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

            Log.i(TAG, "查询 TURN 凭证: $credURL")

            val conn = URL(credURL).openConnection() as HttpURLConnection
            conn.connectTimeout = 5000
            conn.readTimeout = 5000
            conn.requestMethod = "GET"

            if (conn.responseCode != 200) {
                Log.w(TAG, "TURN 凭证查询失败: HTTP ${conn.responseCode}")
                return emptyList()
            }

            val body = conn.inputStream.bufferedReader().use { it.readText() }
            val json = JSONObject(body)

            if (!json.optBoolean("success", false)) {
                Log.w(TAG, "TURN 未配置: ${json.optString("error")}")
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
                        Log.i(TAG, "  TURN: $url → $host")
                    }
                    continue
                }

                try {
                    val addresses = InetAddress.getAllByName(host)
                    for (addr in addresses) {
                        val ip = addr.hostAddress ?: continue
                        if (isIPv4(ip) && !ips.contains(ip)) {
                            ips.add(ip)
                            Log.i(TAG, "  TURN: $url → $host → $ip")
                        }
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "解析 TURN 域名 $host 失败: ${e.message}")
                }
            }
            ips
        } catch (e: Exception) {
            Log.w(TAG, "fetchTURNServerIPs 异常: ${e.message}")
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

    private fun isIPv4(s: String): Boolean {
        val parts = s.split(".")
        if (parts.size != 4) return false
        return parts.all { p ->
            val n = p.toIntOrNull() ?: return false
            n in 0..255
        }
    }

    // ============================================================
    // protected sockets
    // ============================================================

    private fun createProtectedUdpSocket(): Int {
        return try {
            val socket = DatagramSocket()
            socket.reuseAddress = true
            val ok = protect(socket)
            if (!ok) Log.w(TAG, "protect(DatagramSocket) 返回 false")
            protectedUdpSocket = socket
            val pfd = ParcelFileDescriptor.fromDatagramSocket(socket)
            val fd = pfd.detachFd()
            Log.i(TAG, "protected UDP socket fd=$fd")
            fd
        } catch (e: Exception) {
            Log.e(TAG, "createProtectedUdpSocket failed", e)
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
            Log.i(TAG, "protected STUN socket fd=$fd")
            fd
        } catch (e: Exception) {
            Log.e(TAG, "createProtectedStunSocket failed", e)
            -1
        }
    }

    private fun stopVpn() { handleStop() }

    private fun handleStop() {
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
        } catch (e: Exception) { Log.w(TAG, "acquire WakeLock failed", e) }
        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            wifiLock = wm.createWifiLock(
                WifiManager.WIFI_MODE_FULL_HIGH_PERF, "n2n:vpn_wifilock"
            ).apply {
                setReferenceCounted(false)
                acquire()
            }
        } catch (e: Exception) { Log.w(TAG, "acquire WifiLock failed", e) }
    }

    private fun releaseLocks() {
        try { wakeLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) {}
        wakeLock = null
        try { wifiLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) {}
        wifiLock = null
    }

    // ============================================================
    // TUN
    // ============================================================

    private fun buildTunInterface(
        vip: String,
        turnIPs: List<String>,
        preferredIPs: List<String>
    ): ParcelFileDescriptor? {
        return try {
            Log.i(TAG, "建立 TUN（全流量），绑定 IP: $vip")
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
                    excludeIpPrefix(builder, "$ip/32", "preferred")
                }
            }

            builder.setBlocking(true).establish()
        } catch (e: Exception) {
            Log.e(TAG, "establish TUN failed", e)
            null
        }
    }

    private fun excludeIpPrefix(builder: Builder, cidr: String, label: String = "") {
        try {
            val prefix = IpPrefixHelper.parse(cidr)
            builder.excludeRoute(prefix)
            val tag = if (label.isEmpty()) "" else "[$label] "
            Log.i(TAG, "  + excludeRoute: $tag$cidr")
        } catch (e: Exception) {
            Log.w(TAG, "excludeRoute $cidr failed: ${e.message}")
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
        handler.removeCallbacks(updateIpRunnable)
        handleStop()
        super.onDestroy()
    }
}
