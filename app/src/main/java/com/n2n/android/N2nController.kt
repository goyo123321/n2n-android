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

    // ★ 用同一把锁保护 client 的读写，避免 stop() 与 getter 并发时的
    //   JNI 状态不一致（Go 侧 Stop 正在关闭 socket，Kotlin 侧同时读 peersJSON）
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
        if (running.get()) {
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
                } else {
                    synchronized(clientLock) {
                        client = c
                    }
                    running.set(true)
                }
            } catch (t: Throwable) {
                Log.e(TAG, "startAsync failed", t)
                result = "exception: ${t.message}"
            }

            // ★ 回调可能因 Activity 已销毁而抛 IllegalStateException，
            //   这里兜底一下，避免连带把工作线程也炸了
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
     * ★ 改为异步：Go 侧 Stop() 会关闭 WS / TURN / UDP / TUN，
     *   还可能因网络回调卡住几百毫秒。这里不阻塞调用线程（通常是主线程）。
     */
    fun stop() {
        val c: Client?
        synchronized(clientLock) {
            c = client
            client = null
        }
        running.set(false)

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
