package com.n2n.android

import android.content.Context
import android.os.Build
import java.io.File

object ShareDirManager {

    fun getDefaultShareDir(ctx: Context): File {
        val base = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            // Android 10+ 用 Android/media/（MT 管理器、USB 都能访问）
            ctx.getExternalMediaDirs().firstOrNull()
                ?: ctx.getExternalFilesDir(null)
                ?: ctx.filesDir
        } else {
            ctx.getExternalFilesDir(null) ?: ctx.filesDir
        }
        val dir = File(base, "shared")
        if (!dir.exists()) dir.mkdirs()
        return dir
    }

    fun getAbsolutePathForGo(ctx: Context, safUri: String?): String {
        if (safUri.isNullOrEmpty() || safUri.startsWith("content://")) {
            return getDefaultShareDir(ctx).absolutePath
        }
        return safUri
    }
}
