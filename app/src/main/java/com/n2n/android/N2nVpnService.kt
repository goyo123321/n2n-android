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
import com.n2n.mobile.Client
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

        // 前台通知
        startForeground(NOTIF_ID, buildNotification("正在获取虚拟 IP..."))

        val config = Config().apply {
            setSignalingURL(signalingUrl)
            setRoomID(roomId)
            setClientID(clientId)
            setNodeName(nodeName)
            setConnectToken(connectToken)
            setShareDir(shareDir)
        }

        // ★ 后台线程：先 fetch VIP → 建 TUN → start
        Thread {
            Log.i(TAG, "正在获取虚拟 IP...")
            val tmpClient = Client()
            val vip = tmpClient.fetchVirtualIP(config)

            handler.post {
                if (vip.isEmpty()) {
                    Log.e(TAG, "获取虚拟 IP 失败")
                    updateNotification("获取虚拟 IP 失败")
                    handler.postDelayed({ stopVpn() }, 3000)
                    return@post
                }

                Log.i(TAG, "拿到虚拟 IP: $vip")
                updateNotification("虚拟 IP: $vip，正在建立 TUN...")

                // 用这个 IP 建 TUN
                val pfd = buildTunInterface(vip)
                if (pfd == null) {
                    Log.e(TAG, "建立 TUN 失败")
                    updateNotification("建立 TUN 失败")
                    handler.postDelayed({ stopVpn() }, 3000)
                    return@post
                }
                tunInterface = pfd

                // 启动 Go 客户端
                val tunFd = pfd.detachFd()
                N2nController.startAsync(tunFd, config) { err ->
                    handler.post {
                        if (err.isNotEmpty()) {
                            Log.e(TAG, "client start failed: $err")
                            updateNotification("启动失败: $err")
                            handler.postDelayed({ stopVpn() }, 3000)
                        } else {
                            started = true
                            Log.i(TAG, "Go 客户端已启动，等待数据就绪...")
                            handler.post(updateIpRunnable)
                        }
                    }
                }
            }
        }.start()
    }

    private fun stopVpn() {
        handleStop()
    }

    private fun handleStop() {
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

    /**
     * 用服务端分配的虚拟 IP 建 TUN
     */
    private fun buildTunInterface(vip: String): ParcelFileDescriptor? {
        return try {
            Log.i(TAG, "建立 TUN，绑定 IP: $vip")
            Builder()
                .setSession("n2n-client")
                .setMtu(1280)
                .addAddress(vip, 24)
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
