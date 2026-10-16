package com.n2n.android

import android.content.Context
import android.util.Log
import com.n2n.mobile.Client
import com.n2n.mobile.Config
import com.n2n.mobile.Mobile
import com.n2n.mobile.Protector
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

object N2nController {

    private const val TAG = "N2nController"

    @Volatile
    private var client: Client? = null
    private val running = AtomicBoolean(false)
    private val clientLock = Any()

    @Volatile private var tzApplied = false
    @Volatile private var tzAppliedOffset = Long.MIN_VALUE

    @Volatile private var logPathApplied = false

    // ★ Bug 6 修复：标记正在停止，让 handleStart 能等待
    @Volatile private var stopping = false

    fun isRunning(): Boolean = running.get()

    // ★ Bug 6 修复：暴露给 N2nVpnService.handleStart 用
    fun isStopping(): Boolean = stopping

    fun applySystemTimezone() {
        try {
            val tz = java.util.TimeZone.getDefault()
            val offsetMs = tz.getOffset(System.currentTimeMillis())
            val offsetSec = (offsetMs / 1000).toLong()
            if (tzApplied && tzAppliedOffset == offsetSec) return
            Mobile.setTimezoneOffset(offsetSec)
            tzApplied = true
            tzAppliedOffset = offsetSec
            Log.i(TAG, "applySystemTimezone: ${tz.id} offset=${offsetSec}s")
        } catch (t: Throwable) {
            Log.e(TAG, "applySystemTimezone failed", t)
        }
    }

    fun applyLogFilePath(ctx: Context) {
        if (logPathApplied) return
        try {
            val logFile = File(ctx.filesDir, "n2n.log")
            Mobile.setLogFilePath(logFile.absolutePath)
            logPathApplied = true
            Log.i(TAG, "applyLogFilePath: ${logFile.absolutePath}")
        } catch (t: Throwable) {
            Log.e(TAG, "applyLogFilePath failed", t)
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

    // ============================================================
    // ★ Bug 6 修复：stop 加 stopping 标志
    // ============================================================
    //
    // 旧版：running.set(false) 立即置 false，client.stop() 在另一个线程异步
    //       执行。如果用户马上点"启动"，handleStart 看到 !isRunning 就直接
    //       往下走，新 Edge 与旧 Edge 会短暂共存（旧 Edge 还在关 socket、
    //       清理 TURN）。
    //
    // 新版：加 stopping 标志。handleStart 检查 isRunning || isStopping
    //       双重条件，确保旧 Edge 完全清理完再启动新的。
    //
    fun stop() {
        running.set(false)

        val c: Client?
        synchronized(clientLock) {
            c = client
            client = null
        }

        if (c == null) {
            // 没有 client 需要停止，stopping 保持 false
            return
        }

        stopping = true

        Thread {
            try {
                c.stop()
            } catch (t: Throwable) {
                Log.e(TAG, "client.stop failed", t)
            } finally {
                stopping = false
                Log.i(TAG, "stop 完成，stopping=false")
            }
        }.apply { name = "n2n-stop" }.start()
    }

    fun getVirtualIP(): String = try {
        synchronized(clientLock) { client }?.virtualIP ?: ""
    } catch (t: Throwable) {
        Log.e(TAG, "getVirtualIP failed", t)
        ""
    }

    fun getVirtualNetwork(): String = try {
        synchronized(clientLock) { client }?.virtualNetwork ?: ""
    } catch (t: Throwable) {
        Log.e(TAG, "getVirtualNetwork failed", t)
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
