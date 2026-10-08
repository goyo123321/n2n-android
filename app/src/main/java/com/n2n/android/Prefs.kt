package com.n2n.android

import android.content.Context
import java.util.UUID

object Prefs {
    private const val NAME = "n2n_prefs"

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

    // CONNECT_TOKEN 默认空——只有 Worker 端配置了才需要填
    const val DEFAULT_CONNECT_TOKEN = ""

    data class Config(
        val signalingUrl: String,
        val roomId: String,
        val clientId: String,
        val nodeName: String,
        val connectToken: String,
    )

    private fun sp(ctx: Context) = ctx.getSharedPreferences(NAME, Context.MODE_PRIVATE)

    fun load(ctx: Context): Config {
        val s = sp(ctx)
        return Config(
            signalingUrl = s.getString(KEY_SIGNALING_URL, DEFAULT_SIGNALING_URL) ?: DEFAULT_SIGNALING_URL,
            roomId = s.getString(KEY_ROOM_ID, DEFAULT_ROOM_ID) ?: DEFAULT_ROOM_ID,
            clientId = s.getString(KEY_CLIENT_ID, "") ?: "",
            nodeName = s.getString(KEY_NODE_NAME, DEFAULT_NODE_NAME) ?: DEFAULT_NODE_NAME,
            connectToken = s.getString(KEY_CONNECT_TOKEN, DEFAULT_CONNECT_TOKEN) ?: DEFAULT_CONNECT_TOKEN,
        )
    }

    fun save(ctx: Context, cfg: Config) {
        sp(ctx).edit()
            .putString(KEY_SIGNALING_URL, cfg.signalingUrl)
            .putString(KEY_ROOM_ID, cfg.roomId)
            .putString(KEY_CLIENT_ID, cfg.clientId)
            .putString(KEY_NODE_NAME, cfg.nodeName)
            .putString(KEY_CONNECT_TOKEN, cfg.connectToken)
            .apply()
    }

    // ============ 优选 IP ============

    fun loadPreferredIp(ctx: Context): String =
        sp(ctx).getString(KEY_PREFERRED_IP, "") ?: ""

    fun savePreferredIp(ctx: Context, ip: String) {
        sp(ctx).edit().putString(KEY_PREFERRED_IP, ip).apply()
    }

    // ============ 主题 ============

    fun loadThemeMode(ctx: Context): Int = sp(ctx).getInt(KEY_THEME_MODE, 0)

    fun saveThemeMode(ctx: Context, mode: Int) {
        sp(ctx).edit().putInt(KEY_THEME_MODE, mode).apply()
    }

    // ============ 自动 Client ID ============

    fun loadOrCreateClientId(ctx: Context): String {
        val existing = sp(ctx).getString(KEY_AUTO_CLIENT_ID, null)
        if (!existing.isNullOrEmpty()) return existing
        val newId = "android-" + UUID.randomUUID().toString().take(8)
        sp(ctx).edit().putString(KEY_AUTO_CLIENT_ID, newId).apply()
        return newId
    }
}
