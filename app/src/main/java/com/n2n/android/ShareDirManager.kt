package com.n2n.android

import android.content.Context
import android.os.Build
import android.util.Log
import java.io.File

/**
 * 共享目录管理。
 *
 * ## 设计说明
 *
 * Go 侧（netstack 的 HTTP 共享服务）只接受**文件系统绝对路径**，
 * 因此本模块恒定返回默认共享目录，不接受 SAF 的 `content://` URI
 * （Android 10+ Scoped Storage 下无法可靠映射为文件路径）。
 *
 * ## 目录选择
 *
 * - **Android 10+ (API 29+)**：`Android/media/<pkg>/shared/`
 *     - MT 管理器、USB 连电脑可直接访问（无需任何权限）
 *     - 微信/QQ 的文件选择器能看到
 *     - 系统媒体扫描器会索引（用户无需手动授权）
 *
 * - **Android 9 及以下**：`<externalFilesDir>/shared/`
 *     - 无需权限
 *     - 但对外不可见（需要 root 或 adb 才能访问）
 *
 * ## 内存/生命周期
 *
 * `getDefaultShareDir` 每次都会 `mkdirs()`，开销很小（一次 stat），
 * 可在 UI 线程直接调用。目录创建失败时按优先级依次回退：
 *
 *     Android/media  →  externalFiles  →  filesDir（内部存储）
 *
 * 保证至少返回一个可写目录，不会返回 null。
 */
object ShareDirManager {

    private const val TAG = "ShareDirManager"
    private const val SUB_DIR = "shared"

    /**
     * 默认共享目录。
     *
     * 永远返回一个非 null 的 [File]，且调用后保证目录存在（或已尝试创建）。
     * 若所有候选路径都失败，会返回内部 `filesDir/shared/`，
     * 至少在 app 内可用（虽然对外不可见）。
     */
    fun getDefaultShareDir(ctx: Context): File {
        val candidates = buildCandidateBases(ctx)

        for (base in candidates) {
            if (base == null) continue
            try {
                val dir = File(base, SUB_DIR)
                if (!dir.exists()) {
                    if (!dir.mkdirs() && !dir.isDirectory) {
                        Log.w(TAG, "mkdirs 失败: ${dir.absolutePath}")
                        continue
                    }
                }
                if (dir.isDirectory && dir.canWrite()) {
                    return dir
                }
                Log.w(TAG, "目录不可写，尝试下一个: ${dir.absolutePath}")
            } catch (e: Throwable) {
                Log.w(TAG, "检查候选目录失败: ${e.message}")
            }
        }

        // 理论上不会走到这里（filesDir 一定可写），但作为最终兜底
        val fallback = File(ctx.filesDir, SUB_DIR)
        try {
            if (!fallback.exists()) fallback.mkdirs()
        } catch (_: Throwable) {}
        Log.w(TAG, "所有候选目录失败，回退到内部存储: ${fallback.absolutePath}")
        return fallback
    }

    /**
     * 给 Go 侧使用的共享目录绝对路径。
     *
     * **固定返回默认共享目录**，忽略传入的 SAF URI。
     *
     * 参数 `safUri` 保留仅为兼容旧调用方签名，当前实现不使用。
     * 若未来需要支持自定义目录，需要把 Go 侧的 `StartShareServer(rootDir string)`
     * 改为接受 `DocumentFile` 抽象（改动较大），否则 SAF 结果无法生效。
     */
    fun getAbsolutePathForGo(
        ctx: Context,
        @Suppress("UNUSED_PARAMETER") safUri: String?
    ): String {
        return getDefaultShareDir(ctx).absolutePath
    }

    /**
     * 判断给定的绝对路径是否在共享目录内。
     *
     * 用于「管理文件」页面的安全检查，防止越权访问。
     */
    fun isInsideShareDir(ctx: Context, path: String): Boolean {
        return try {
            val root = getDefaultShareDir(ctx).canonicalPath
            val target = File(path).canonicalPath
            target == root || target.startsWith(root + File.separator)
        } catch (e: Throwable) {
            false
        }
    }

    // ============================================================
    // 内部：按优先级构造候选基础目录
    // ============================================================

    /**
     * 候选基础目录，按优先级从高到低：
     *
     * 1. Android 10+：`getExternalMediaDirs()[0]`  → `Android/media/<pkg>/`
     * 2. 任意版本：`getExternalFilesDir(null)`     → `Android/data/<pkg>/files/`
     * 3. 最终兜底：`filesDir`                       → 内部存储
     *
     * `getExternalMediaDirs` 是 Android 10+ 才有，且返回数组第一项是主用户
     * 路径；在 work profile / 多用户下会不同，这里只取第一项即可。
     */
    private fun buildCandidateBases(ctx: Context): List<File?> {
        val list = mutableListOf<File?>()

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            try {
                val mediaDirs = ctx.getExternalMediaDirs()
                // 有些设备（如模拟器）可能返回空数组，务必判空
                if (mediaDirs != null && mediaDirs.isNotEmpty()) {
                    list.add(mediaDirs[0])
                }
            } catch (e: Throwable) {
                Log.w(TAG, "getExternalMediaDirs 失败: ${e.message}")
            }
        }

        try {
            list.add(ctx.getExternalFilesDir(null))
        } catch (e: Throwable) {
            Log.w(TAG, "getExternalFilesDir 失败: ${e.message}")
        }

        // filesDir 一定非 null，作为最终兜底
        list.add(ctx.filesDir)

        return list
    }
}
