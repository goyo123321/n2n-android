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

    // 用同一把锁保护 client 的读写
    private val clientLock = Any()

    fun isRunning(): Boolean = running.get()

    fun startAsync(
        tunFd: Int,
        udpFd: Int,
        stunFd: Int,
        config: Config,
        protector: Protector,
        onResult: (String) -> Unit
    ) {
        // ★ 修复 1：CAS 原子占用 running。
        //    之前是 if (running.get()) 判断后再 running.set(true)，
        //    两个并发调用可能都通过检查。CAS 保证只有一个能进入。
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
                    // ★ 修复 2：启动期间可能被 stop() 调过。
                    //    在 clientLock 内检查 running 是否仍为 true，
                    //    是 → 发布 client；否 → 回收刚起的 Client。
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

    /**
     * 停止 VPN。
     *
     * ★ 修复 3：先置 running=false（无锁），再取 client。
     *    这样 startAsync 里的 `if (running.get())` 检查与
     *    stop() 里的 `running.set(false)` 有明确的先后关系，
     *    不会出现"启动检查通过 → 停止置 false → 启动发布 client"的漏网。
     */
    fun stop() {
        // 先置 false：让任何正在跑的 startAsync 检查到"已被停止"
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

    // ============================================================
    // getter：全部 try-catch，跨 JNI 调用不能把异常抛回 UI
    // ============================================================

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
