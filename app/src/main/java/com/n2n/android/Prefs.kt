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
    private const val KEY_CONNECT_TOKEN = "connect_token"     // ★ 原 uuid
    private const val KEY_SHARE_DIR_URI = "share_dir_uri"
    private const val KEY_THEME_MODE = "theme_mode"
    private const val KEY_FIRST_LAUNCH = "first_launch_done"
    private const val KEY_AUTO_CLIENT_ID = "auto_client_id"

    private const val DEFAULT_SIGNALING_URL = ""
    private const val DEFAULT_ROOM_ID = "default-room"
    private const val DEFAULT_NODE_NAME = "Android"

    // ★ 与服务端 DEFAULT_CONNECT_TOKEN 保持一致
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

    // ============ 共享目录 URI（保留占位，功能已停用）============

    fun loadShareDirUri(ctx: Context): String? =
        sp(ctx).getString(KEY_SHARE_DIR_URI, null)

    fun saveShareDirUri(ctx: Context, uri: String?) {
        sp(ctx).edit().apply {
            if (uri.isNullOrEmpty()) remove(KEY_SHARE_DIR_URI)
            else putString(KEY_SHARE_DIR_URI, uri)
        }.apply()
    }

    // ============ 主题 ============

    fun loadThemeMode(ctx: Context): Int = sp(ctx).getInt(KEY_THEME_MODE, 0)

    fun saveThemeMode(ctx: Context, mode: Int) {
        sp(ctx).edit().putInt(KEY_THEME_MODE, mode).apply()
    }

    // ============ 首次启动 ============

    fun isFirstLaunch(ctx: Context): Boolean =
        !sp(ctx).getBoolean(KEY_FIRST_LAUNCH, false)

    fun setFirstLaunchDone(ctx: Context) {
        sp(ctx).edit().putBoolean(KEY_FIRST_LAUNCH, true).apply()
    }

    // ============ 自动 Client ID ============

    fun loadOrCreateClientId(ctx: Context): String {
        val existing = sp(ctx).getString(KEY_AUTO_CLIENT_ID, null)
        if (!existing.isNullOrEmpty()) return existing
        val newId = "android-" + UUID.randomUUID().toString().take(8)
        sp(ctx).edit().putString(KEY_AUTO_CLIENT_ID, newId).apply()
        return newId
    }

    // ============ 迁移：老的 uuid / connect_token 统一到 connect_token ============

    fun migrateLegacy(ctx: Context) {
        val s = sp(ctx)
        val editor = s.edit()
        var dirty = false

        // 情况 1：已有 connect_token，删掉遗留的 uuid
        val hasNew = !s.getString(KEY_CONNECT_TOKEN, null).isNullOrEmpty()
        if (hasNew) {
            if (s.contains("uuid")) {
                editor.remove("uuid")
                dirty = true
            }
        } else {
            // 情况 2：没有 connect_token，尝试从 uuid 迁移
            val legacyUuid = s.getString("uuid", null)
            if (!legacyUuid.isNullOrEmpty()) {
                editor.putString(KEY_CONNECT_TOKEN, legacyUuid)
                dirty = true
            }
            if (s.contains("uuid")) {
                editor.remove("uuid")
                dirty = true
            }
        }

        if (dirty) editor.apply()
    }
}
