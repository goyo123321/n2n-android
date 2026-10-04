package com.n2n.android

import com.n2n.mobile.Client
import com.n2n.mobile.Config
import com.n2n.mobile.ProgressListener
import java.util.concurrent.atomic.AtomicBoolean

object N2nController {

    private var client: Client? = null
    private val running = AtomicBoolean(false)

    fun isRunning(): Boolean = running.get()

    fun start(tunFd: Int, config: Config): String {
        if (running.get()) return "already running"

        val c = Client()
        c.setTunFD(tunFd.toLong())
        val err = c.start(config)
        if (err.isNotEmpty()) {
            return "start failed: $err"
        }
        client = c
        running.set(true)
        return ""
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

    fun setProgressListener(l: ProgressListener) {
        client?.setProgressListener(l)
    }
}
