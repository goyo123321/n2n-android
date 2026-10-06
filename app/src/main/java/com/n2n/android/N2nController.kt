package com.n2n.android

import com.n2n.mobile.Client
import com.n2n.mobile.Config
import java.util.concurrent.atomic.AtomicBoolean

object N2nController {

    private var client: Client? = null
    private val running = AtomicBoolean(false)

    fun isRunning(): Boolean = running.get()

    fun start(tunFd: Int, udpFd: Int, stunFd: Int, config: Config): String {
        if (running.get()) return "already running"
        val c = Client()
        c.setTunFD(tunFd.toLong())
        if (udpFd > 0) c.setUdpFD(udpFd.toLong())
        if (stunFd > 0) c.setStunFD(stunFd.toLong())
        val err = c.start(config)
        if (err.isNotEmpty()) return "start failed: $err"
        client = c
        running.set(true)
        return ""
    }

    fun startAsync(tunFd: Int, udpFd: Int, stunFd: Int, config: Config, onResult: (String) -> Unit) {
        if (running.get()) {
            onResult("already running")
            return
        }
        Thread {
            val c = Client()
            c.setTunFD(tunFd.toLong())
            if (udpFd > 0) c.setUdpFD(udpFd.toLong())
            if (stunFd > 0) c.setStunFD(stunFd.toLong())
            val err = c.start(config)
            if (err.isNotEmpty()) {
                onResult("start failed: $err")
                return@Thread
            }
            client = c
            running.set(true)
            onResult("")
        }.start()
    }

    fun stop() {
        client?.stop()
        client = null
        running.set(false)
    }

    fun getStatus(): String = client?.status ?: "not running"
    fun getVirtualIP(): String = client?.virtualIP ?: ""
    fun getClientID(): String = client?.clientID ?: ""
    fun getPeersJSON(): String = client?.peersJSON ?: "[]"

    fun getLogs(): String {
        return try { com.n2n.mobile.Mobile.getLogs() ?: "" } catch (e: Exception) { "" }
    }

    fun clearLogs() {
        try { com.n2n.mobile.Mobile.clearLogs() } catch (e: Exception) {}
    }
}
