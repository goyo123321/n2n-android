package com.n2n.android

import android.content.Context
import java.util.UUID

object Prefs {
    private const val NAME = "n2n_prefs"
    private const val FALLBACK_NAME = "n2n_prefs_fallback"

    private const val KEY_SIGNALING_URL = "signaling_url"
    private const val KEY_PREFERRED_IP = "preferred_ip"
    private const val KEY_ROOM_ID = "room_id"
    private const val KEY_CLIENT_ID = "client_id"
    private const val KEY_NODE_NAME = "node_name"
    private const val KEY_CONNECT_TOKEN = "connect_token"
    private const val KEY_THEME_MODE = "theme_mode"
    private const val KEY_AUTO_CLIENT_ID = "auto_client_id"

    private const val DEFAULT_SIGNALING_URL = ""
    private const val DEFAULT_ROOM_ID = "default-room"
    private const val DEFAULT_NODE_NAME = "Android"

    // ★ 从 public const 降为 private：全项目只有本文件的 defaultConfig() 用，
    //   外部没有调用方，public 暴露没有意义
    private const val DEFAULT_CONNECT_TOKEN = ""

    data class Config(
        val signalingUrl: String,
        val roomId: String,
        val clientId: String,
        val nodeName: String,
        val connectToken: String,
    )

    // 双层 fallback：主 SP 打不开时降级到备用 SP，保证 UI 不崩
    private fun sp(ctx: Context) =
        try {
            ctx.getSharedPreferences(NAME, Context.MODE_PRIVATE)
        } catch (t: Throwable) {
            try {
                ctx.getSharedPreferences(FALLBACK_NAME, Context.MODE_PRIVATE)
            } catch (t2: Throwable) {
                null
            }
        }

    fun load(ctx: Context): Config {
        return try {
            val s = sp(ctx) ?: return defaultConfig()
            Config(
                signalingUrl = s.getString(KEY_SIGNALING_URL, DEFAULT_SIGNALING_URL) ?: DEFAULT_SIGNALING_URL,
                roomId = s.getString(KEY_ROOM_ID, DEFAULT_ROOM_ID) ?: DEFAULT_ROOM_ID,
                clientId = s.getString(KEY_CLIENT_ID, "") ?: "",
                nodeName = s.getString(KEY_NODE_NAME, DEFAULT_NODE_NAME) ?: DEFAULT_NODE_NAME,
                connectToken = s.getString(KEY_CONNECT_TOKEN, DEFAULT_CONNECT_TOKEN) ?: DEFAULT_CONNECT_TOKEN,
            )
        } catch (t: Throwable) {
            defaultConfig()
        }
    }

    private fun defaultConfig() = Config(
        signalingUrl = DEFAULT_SIGNALING_URL,
        roomId = DEFAULT_ROOM_ID,
        clientId = "",
        nodeName = DEFAULT_NODE_NAME,
        connectToken = DEFAULT_CONNECT_TOKEN,
    )

    fun save(ctx: Context, cfg: Config) {
        try {
            sp(ctx)?.edit()
                ?.putString(KEY_SIGNALING_URL, cfg.signalingUrl)
                ?.putString(KEY_ROOM_ID, cfg.roomId)
                ?.putString(KEY_CLIENT_ID, cfg.clientId)
                ?.putString(KEY_NODE_NAME, cfg.nodeName)
                ?.putString(KEY_CONNECT_TOKEN, cfg.connectToken)
                ?.apply()
        } catch (_: Throwable) {}
    }

    // ============ 优选 IP ============

    fun loadPreferredIp(ctx: Context): String =
        try {
            sp(ctx)?.getString(KEY_PREFERRED_IP, "") ?: ""
        } catch (t: Throwable) {
            ""
        }

    fun savePreferredIp(ctx: Context, ip: String) {
        try {
            sp(ctx)?.edit()?.putString(KEY_PREFERRED_IP, ip)?.apply()
        } catch (_: Throwable) {}
    }

    // ============ 主题 ============

    fun loadThemeMode(ctx: Context): Int =
        try {
            sp(ctx)?.getInt(KEY_THEME_MODE, 0) ?: 0
        } catch (t: Throwable) {
            0
        }

    fun saveThemeMode(ctx: Context, mode: Int) {
        try {
            sp(ctx)?.edit()?.putInt(KEY_THEME_MODE, mode)?.apply()
        } catch (_: Throwable) {}
    }

    // ============ 自动 Client ID ============

    fun loadOrCreateClientId(ctx: Context): String {
        return try {
            val s = sp(ctx) ?: return generateFallbackId()
            val existing = s.getString(KEY_AUTO_CLIENT_ID, null)
            if (!existing.isNullOrEmpty()) return existing
            val newId = generateFallbackId()
            s.edit().putString(KEY_AUTO_CLIENT_ID, newId).apply()
            newId
        } catch (t: Throwable) {
            generateFallbackId()
        }
    }

    // 用 hex 而不是 take(8)：UUID 里 '-' 不会落在前 8 位，但显式过滤更稳
    private fun generateFallbackId(): String {
        val raw = UUID.randomUUID().toString().replace("-", "")
        return "android-" + raw.take(8)
    }
}
