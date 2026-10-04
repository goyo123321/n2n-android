package com.n2n.android

import android.content.Context
import android.content.Intent
import android.app.PendingIntent
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import com.n2n.mobile.ProgressListener
import java.util.concurrent.ConcurrentHashMap

/**
 * 接收 PC 上传到手机共享目录的进度通知。
 *
 * 由 Go 侧通过 gomobile callback 触发：
 *   - onUploadStart    → 创建通知
 *   - onUploadProgress → 更新进度
 *   - onUploadComplete → 标记成功
 *   - onUploadError    → 标记失败
 *
 * 注意：gomobile 会把 Go 的 int 映射成 Kotlin 的 Long。
 */
class UploadProgressListener(private val ctx: Context) : ProgressListener() {

    companion object {
        private const val CHANNEL_ID = "n2n_upload"
        private const val ID_BASE = 2000
        private const val ID_RANGE = 100
    }

    // 文件名 → 通知 ID
    private val idMap = ConcurrentHashMap<String, Int>()
    private var nextId = ID_BASE

    private fun idFor(filename: String): Int {
        return idMap.getOrPut(filename) {
            var id = nextId
            nextId++
            if (nextId >= ID_BASE + ID_RANGE) nextId = ID_BASE
            // 避免和已有 ID 冲突
            while (idMap.containsValue(id)) {
                id++
                if (id >= ID_BASE + ID_RANGE) id = ID_BASE
            }
            id
        }
    }

    // ============================================================
    // Go → Kotlin 回调
    // ============================================================

    override fun onUploadStart(filename: String, totalBytes: Long) {
        showProgressNotif(filename, 0L, totalBytes, "准备接收")
    }

    override fun onUploadProgress(filename: String, received: Long, totalBytes: Long) {
        showProgressNotif(filename, received, totalBytes, "接收中")
    }

    override fun onUploadComplete(filename: String, received: Long, success: Boolean) {
        val nm = NotificationManagerCompat.from(ctx)
        val id = idFor(filename)

        if (success) {
            val notif = NotificationCompat.Builder(ctx, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.stat_sys_download_done)
                .setContentTitle("✅ 文件已接收")
                .setContentText("$filename  (${fmtSize(received)})")
                .setContentIntent(buildContentIntent())
                .setAutoCancel(true)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .build()
            safeNotify(nm, id, notif)
        } else {
            nm.cancel(id)
        }
        idMap.remove(filename)
    }

    override fun onUploadError(filename: String, message: String) {
        val nm = NotificationManagerCompat.from(ctx)
        val id = idFor(filename)

        val notif = NotificationCompat.Builder(ctx, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_notify_error)
            .setContentTitle("❌ 接收失败")
            .setContentText("$filename: $message")
            .setContentIntent(buildContentIntent())
            .setAutoCancel(true)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .build()
        safeNotify(nm, id, notif)
        idMap.remove(filename)
    }

    // ============================================================
    // 内部
    // ============================================================

    private fun showProgressNotif(
        filename: String,
        cur: Long,
        total: Long,
        status: String,
    ) {
        val nm = NotificationManagerCompat.from(ctx)
        val id = idFor(filename)

        val percent = if (total > 0) ((cur * 100) / total).toInt() else 0
        val text = if (total > 0) {
            "$status · ${fmtSize(cur)} / ${fmtSize(total)}  ($percent%)"
        } else {
            "$status · ${fmtSize(cur)}"
        }

        val notif = NotificationCompat.Builder(ctx, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentTitle("📥 正在接收: $filename")
            .setContentText(text)
            .setContentIntent(buildContentIntent())
            .setProgress(100, percent, total <= 0)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
        safeNotify(nm, id, notif)
    }

    private fun buildContentIntent(): PendingIntent {
        // 点击通知 → 打开文件管理
        val intent = Intent(ctx, FileManagerActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        return PendingIntent.getActivity(
            ctx,
            0,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
    }

    private fun safeNotify(nm: NotificationManagerCompat, id: Int, notif: android.app.Notification) {
        // Android 13+ 未授予 POST_NOTIFICATIONS 时会抛 SecurityException
        try {
            nm.notify(id, notif)
        } catch (_: SecurityException) {
            // 用户拒绝了通知权限，静默忽略
        }
    }

    private fun fmtSize(b: Long): String {
        return when {
            b < 1024L -> "$b B"
            b < 1048576L -> "%.1f KB".format(b / 1024.0)
            b < 1073741824L -> "%.1f MB".format(b / 1048576.0)
            else -> "%.2f GB".format(b / 1073741824.0)
        }
    }
}
