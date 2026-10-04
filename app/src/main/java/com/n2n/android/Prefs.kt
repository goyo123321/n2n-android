package com.n2n.android

import android.content.Context
import java.util.UUID

object Prefs {
    private const val NAME = "n2n_prefs"

    private const val KEY_SIGNALING_URL = "signaling_url"
    private const val KEY_ROOM_ID = "room_id"
    private const val KEY_CLIENT_ID = "client_id"
    private const val KEY_NODE_NAME = "node_name"
    private const val KEY_CONNECT_TOKEN = "connect_token"
    private const val KEY_SHARE_DIR_URI = "share_dir_uri"
    private const val KEY_THEME_MODE = "theme_mode"
    private const val KEY_FIRST_LAUNCH = "first_launch_done"
    private const val KEY_AUTO_CLIENT_ID = "auto_client_id"

    // 默认值
    private const val DEFAULT_SIGNALING_URL = ""   // 空 → 首次启动提示用户填
    private const val DEFAULT_ROOM_ID = "default-room"
    private const val DEFAULT_NODE_NAME = "Android"

    data class Config(
        val signalingUrl: String,
        val roomId: String,
        val clientId: String,
        val nodeName: String,
        val connectToken: String,
    )

    private fun sp(ctx: Context) = ctx.getSharedPreferences(NAME, Context.MODE_PRIVATE)

    // ============================================================
    // Config 读写
    // ============================================================

    fun load(ctx: Context): Config {
        val s = sp(ctx)
        return Config(
            signalingUrl = s.getString(KEY_SIGNALING_URL, DEFAULT_SIGNALING_URL) ?: DEFAULT_SIGNALING_URL,
            roomId = s.getString(KEY_ROOM_ID, DEFAULT_ROOM_ID) ?: DEFAULT_ROOM_ID,
            clientId = s.getString(KEY_CLIENT_ID, "") ?: "",
            nodeName = s.getString(KEY_NODE_NAME, DEFAULT_NODE_NAME) ?: DEFAULT_NODE_NAME,
            connectToken = s.getString(KEY_CONNECT_TOKEN, "") ?: "",
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

    // ============================================================
    // 共享目录 URI（SAF 选择后保存）
    // ============================================================

    fun loadShareDirUri(ctx: Context): String? =
        sp(ctx).getString(KEY_SHARE_DIR_URI, null)

    fun saveShareDirUri(ctx: Context, uri: String?) {
        sp(ctx).edit().apply {
            if (uri.isNullOrEmpty()) remove(KEY_SHARE_DIR_URI)
            else putString(KEY_SHARE_DIR_URI, uri)
        }.apply()
    }

    // ============================================================
    // 主题模式
    // 0 = 跟随系统，1 = 浅色，2 = 深色
    // ============================================================

    fun loadThemeMode(ctx: Context): Int = sp(ctx).getInt(KEY_THEME_MODE, 0)

    fun saveThemeMode(ctx: Context, mode: Int) {
        sp(ctx).edit().putInt(KEY_THEME_MODE, mode).apply()
    }

    // ============================================================
    // 首次启动标记
    // ============================================================

    fun isFirstLaunch(ctx: Context): Boolean =
        !sp(ctx).getBoolean(KEY_FIRST_LAUNCH, false)

    fun setFirstLaunchDone(ctx: Context) {
        sp(ctx).edit().putBoolean(KEY_FIRST_LAUNCH, true).apply()
    }

    // ============================================================
    // ★ 自动生成 Client ID（首次调用生成，后续复用）
    // 保证同一台设备重启后 Client ID 不变 → 虚拟 IP 不变
    // ============================================================

    fun loadOrCreateClientId(ctx: Context): String {
        val existing = sp(ctx).getString(KEY_AUTO_CLIENT_ID, null)
        if (!existing.isNullOrEmpty()) return existing

        val newId = "android-" + UUID.randomUUID().toString().take(8)
        sp(ctx).edit().putString(KEY_AUTO_CLIENT_ID, newId).apply()
        return newId
    }

    /**
     * 手动重置 Client ID（用于"清除数据"等场景）
     */
    fun resetClientId(ctx: Context) {
        sp(ctx).edit().remove(KEY_AUTO_CLIENT_ID).apply()
    }
}
