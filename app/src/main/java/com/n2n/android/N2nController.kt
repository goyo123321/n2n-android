package com.n2n.android

import android.util.Log
import com.n2n.mobile.Client
import com.n2n.mobile.Config
import com.n2n.mobile.Mobile
import com.n2n.mobile.Protector
import java.util.concurrent.atomic.AtomicBoolean

object N2nController {

    private const val TAG = "N2nController"

    // ★ @Volatile：start 线程写入，UI / 后台线程读取，避免可见性问题
    @Volatile
    private var client: Client? = null
    private val running = AtomicBoolean(false)

    fun isRunning(): Boolean = running.get()

    fun start(tunFd: Int, udpFd: Int, stunFd: Int, protector: Protector, config: Config): String {
        if (running.get()) return "already running"
        val c = Client()
        c.setTunFD(tunFd.toLong())
        if (udpFd > 0) c.setUdpFD(udpFd.toLong())
        if (stunFd > 0) c.setStunFD(stunFd.toLong())
        c.setProtector(protector)
        val err = c.start(config)
        if (err.isNotEmpty()) return "start failed: $err"
        client = c
        running.set(true)
        return ""
    }

    fun startAsync(
        tunFd: Int,
        udpFd: Int,
        stunFd: Int,
        config: Config,
        protector: Protector,
        onResult: (String) -> Unit
    ) {
        if (running.get()) {
            onResult("already running")
            return
        }
        Thread {
            try {
                val c = Client()
                c.setTunFD(tunFd.toLong())
                if (udpFd > 0) c.setUdpFD(udpFd.toLong())
                if (stunFd > 0) c.setStunFD(stunFd.toLong())
                c.setProtector(protector)
                val err = c.start(config)
                if (err.isNotEmpty()) {
                    onResult("start failed: $err")
                    return@Thread
                }
                client = c
                running.set(true)
                onResult("")
            } catch (t: Throwable) {
                Log.e(TAG, "startAsync failed", t)
                onResult("exception: ${t.message}")
            }
        }.start()
    }

    fun stop() {
        try {
            client?.stop()
        } catch (t: Throwable) {
            Log.e(TAG, "stop failed", t)
        }
        client = null
        running.set(false)
    }

    fun getStatus(): String = client?.status ?: "not running"
    fun getVirtualIP(): String = client?.virtualIP ?: ""
    fun getClientID(): String = client?.clientID ?: ""
    fun getPeersJSON(): String = client?.peersJSON ?: "[]"

    fun getLogs(): String {
        return try {
            Mobile.getLogs() ?: ""
        } catch (e: Exception) {
            ""
        }
    }

    fun clearLogs() {
        try {
            Mobile.clearLogs()
        } catch (e: Exception) {
            // ignore
        }
    }
}
