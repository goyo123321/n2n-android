package com.n2n.android

import android.content.Context

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

    private const val DEFAULT_SIGNALING_URL = "wss://edge-signal.xloavnui.workers.dev"
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

    fun loadShareDirUri(ctx: Context): String? =
        sp(ctx).getString(KEY_SHARE_DIR_URI, null)

    fun saveShareDirUri(ctx: Context, uri: String?) {
        sp(ctx).edit().apply {
            if (uri.isNullOrEmpty()) remove(KEY_SHARE_DIR_URI)
            else putString(KEY_SHARE_DIR_URI, uri)
        }.apply()
    }

    fun loadThemeMode(ctx: Context): Int = sp(ctx).getInt(KEY_THEME_MODE, 0)

    fun saveThemeMode(ctx: Context, mode: Int) {
        sp(ctx).edit().putInt(KEY_THEME_MODE, mode).apply()
    }

    fun isFirstLaunch(ctx: Context): Boolean =
        !sp(ctx).getBoolean(KEY_FIRST_LAUNCH, false)

    fun setFirstLaunchDone(ctx: Context) {
        sp(ctx).edit().putBoolean(KEY_FIRST_LAUNCH, true).apply()
    }
}
