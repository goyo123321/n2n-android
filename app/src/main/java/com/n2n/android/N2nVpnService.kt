package com.n2n.android

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Intent
import android.net.VpnService
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.util.Log
import androidx.core.app.NotificationCompat
import com.n2n.mobile.Config

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
    }

    private var tunInterface: ParcelFileDescriptor? = null
    private var started = false

    private val handler = Handler(Looper.getMainLooper())

    /** ★ 轮询虚拟 IP，等 Go 端拿到后更新通知 */
    private val updateIpRunnable = object : Runnable {
        private var attempts = 0
        override fun run() {
            attempts++
            val ip = N2nController.getVirtualIP()
            if (ip.isNotEmpty()) {
                updateNotification("已连接 · 虚拟 IP $ip")
                Log.i(TAG, "VPN 虚拟 IP: $ip")
            } else if (attempts < 30) {
                // 最多轮询 30 次（30 秒）
                updateNotification("正在连接... (${attempts}s)")
                handler.postDelayed(this, 1000)
            } else {
                updateNotification("连接超时")
                Log.w(TAG, "等待虚拟 IP 超时")
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
        if (started) {
            Log.w(TAG, "already started")
            return
        }

        val signalingUrl = intent.getStringExtra(EXTRA_SIGNALING_URL) ?: run {
            Log.e(TAG, "missing signaling_url")
            stopSelf()
            return
        }
        val roomId = intent.getStringExtra(EXTRA_ROOM_ID) ?: "default-room"
        val clientId = intent.getStringExtra(EXTRA_CLIENT_ID) ?: ""
        val nodeName = intent.getStringExtra(EXTRA_NODE_NAME) ?: "Android"
        val connectToken = intent.getStringExtra(EXTRA_CONNECT_TOKEN) ?: ""
        val shareDir = intent.getStringExtra(EXTRA_SHARE_DIR)
            ?: ShareDirManager.getDefaultShareDir(this).absolutePath

        // 1. 建立 TUN
        val pfd = buildTunInterface()
        if (pfd == null) {
            Log.e(TAG, "failed to establish TUN")
            stopSelf()
            return
        }
        tunInterface = pfd

        // 2. 前台通知（先显示"正在启动"）
        startForeground(NOTIF_ID, buildNotification("正在启动..."))

        // 3. 组装 Config
        val config = Config().apply {
            setSignalingURL(signalingUrl)
            setRoomID(roomId)
            setClientID(clientId)
            setNodeName(nodeName)
            setConnectToken(connectToken)
            setShareDir(shareDir)
        }

        val tunFd = pfd.detachFd()

        // ★ 4. 异步启动 Go 客户端（不在主线程）
        N2nController.startAsync(tunFd, config) { err ->
            handler.post {
                if (err.isNotEmpty()) {
                    Log.e(TAG, "client start failed: $err")
                    updateNotification("启动失败: $err")
                    handler.postDelayed({ stopVpn() }, 3000)
                } else {
                    started = true
                    Log.i(TAG, "Go 客户端已启动，等待虚拟 IP...")
                    // ★ 启动轮询虚拟 IP
                    handler.post(updateIpRunnable)
                }
            }
        }
    }

    private fun stopVpn() {
        handleStop()
    }

    private fun handleStop() {
        // 停止 IP 轮询
        handler.removeCallbacks(updateIpRunnable)

        if (started) {
            N2nController.stop()
            started = false
        }
        try {
            tunInterface?.close()
        } catch (_: Exception) {}
        tunInterface = null

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            stopForeground(STOP_FOREGROUND_REMOVE)
        } else {
            @Suppress("DEPRECATION")
            stopForeground(true)
        }
        stopSelf()
    }

    private fun buildTunInterface(): ParcelFileDescriptor? {
        return try {
            Builder()
                .setSession("n2n-client")
                .setMtu(1280)
                .addAddress("10.64.0.2", 24)
                .addRoute("10.64.0.0", 24)
                .addDnsServer("1.1.1.1")
                .setBlocking(true)
                .establish()
        } catch (e: Exception) {
            Log.e(TAG, "establish TUN failed", e)
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
                    NOTIF_CHANNEL_ID,
                    "n2n VPN",
                    NotificationManager.IMPORTANCE_LOW
                ).apply {
                    description = "n2n 组网运行状态"
                }
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
