package com.n2n.android

import android.util.Log
import com.n2n.mobile.Client
import com.n2n.mobile.Config
import com.n2n.mobile.Mobile
import com.n2n.mobile.Protector
import java.util.concurrent.atomic.AtomicBoolean

object N2nController {

    private const val TAG = "N2nController"

    @Volatile
    private var client: Client? = null
    private val running = AtomicBoolean(false)
    private val clientLock = Any()

    // ★ 时区：只在第一次调用时生效，之后忽略（除非偏移量变化）
    @Volatile private var tzApplied = false
    @Volatile private var tzAppliedOffset = Int.MIN_VALUE

    fun isRunning(): Boolean = running.get()

    /**
     * 把设备当前时区偏移量传给 Go 侧。
     *
     * gomobile 环境下 Go 的 time.Local 默认为 UTC，无法读取 Android
     * 系统时区。Kotlin 侧读 TimeZone.getDefault() 后传给 Go。
     *
     * 幂等：偏移量没变时跳过。
     */
    fun applySystemTimezone() {
        try {
            val tz = java.util.TimeZone.getDefault()
            val offsetMs = tz.getOffset(System.currentTimeMillis())
            val offsetSec = offsetMs / 1000
            if (tzApplied && tzAppliedOffset == offsetSec) return
            Mobile.setTimezoneOffset(offsetSec)
            tzApplied = true
            tzAppliedOffset = offsetSec
            Log.i(TAG, "applySystemTimezone: ${tz.id} offset=${offsetSec}s")
        } catch (t: Throwable) {
            Log.e(TAG, "applySystemTimezone failed", t)
        }
    }

    fun startAsync(
        tunFd: Int,
        udpFd: Int,
        stunFd: Int,
        config: Config,
        protector: Protector,
        onResult: (String) -> Unit
    ) {
        if (!running.compareAndSet(false, true)) {
            onResult("already running")
            return
        }

        Thread {
            var result = ""
            try {
                val c = Client()
                c.setTunFD(tunFd.toLong())
                if (udpFd > 0) c.setUdpFD(udpFd.toLong())
                if (stunFd > 0) c.setStunFD(stunFd.toLong())
                try {
                    c.setProtector(protector)
                } catch (t: Throwable) {
                    Log.e(TAG, "setProtector failed", t)
                }

                val err = c.start(config)
                if (err.isNotEmpty()) {
                    result = "start failed: $err"
                    running.set(false)
                } else {
                    var accepted = false
                    synchronized(clientLock) {
                        if (running.get()) {
                            client = c
                            accepted = true
                        }
                    }
                    if (!accepted) {
                        Log.w(TAG, "startAsync 完成时 running 已被置 false，回收 Client")
                        try {
                            c.stop()
                        } catch (t: Throwable) {
                            Log.e(TAG, "cleanup stop failed", t)
                        }
                    }
                }
            } catch (t: Throwable) {
                Log.e(TAG, "startAsync failed", t)
                result = "exception: ${t.message}"
                running.set(false)
            }

            try {
                onResult(result)
            } catch (t: Throwable) {
                Log.e(TAG, "onResult callback failed", t)
            }
        }.apply { name = "n2n-start" }.start()
    }

    fun stop() {
        running.set(false)

        val c: Client?
        synchronized(clientLock) {
            c = client
            client = null
        }

        if (c == null) return

        Thread {
            try {
                c.stop()
            } catch (t: Throwable) {
                Log.e(TAG, "client.stop failed", t)
            }
        }.apply { name = "n2n-stop" }.start()
    }

    fun getVirtualIP(): String = try {
        synchronized(clientLock) { client }?.virtualIP ?: ""
    } catch (t: Throwable) {
        Log.e(TAG, "getVirtualIP failed", t)
        ""
    }

    fun getClientID(): String = try {
        synchronized(clientLock) { client }?.clientID ?: ""
    } catch (t: Throwable) {
        Log.e(TAG, "getClientID failed", t)
        ""
    }

    fun getPeersJSON(): String = try {
        synchronized(clientLock) { client }?.peersJSON ?: "[]"
    } catch (t: Throwable) {
        Log.e(TAG, "getPeersJSON failed", t)
        "[]"
    }

    fun getLogs(): String {
        return try {
            Mobile.getLogs() ?: ""
        } catch (t: Throwable) {
            Log.e(TAG, "getLogs failed", t)
            ""
        }
    }

    fun clearLogs() {
        try {
            Mobile.clearLogs()
        } catch (t: Throwable) {
            Log.e(TAG, "clearLogs failed", t)
        }
    }
}
