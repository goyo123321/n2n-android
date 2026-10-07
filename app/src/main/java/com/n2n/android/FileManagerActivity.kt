package com.n2n.android

import android.content.ContentValues
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.provider.MediaStore
import android.provider.OpenableColumns
import android.util.Log
import android.view.View
import android.widget.EditText
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.annotation.RequiresApi
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.FileProvider
import androidx.recyclerview.widget.LinearLayoutManager
import com.n2n.android.databinding.ActivityFileManagerBinding
import java.io.File

class FileManagerActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "FileManager"
    }

    private lateinit var binding: ActivityFileManagerBinding
    private lateinit var adapter: FileAdapter
    private lateinit var rootDir: File
    private var currentDir: File? = null

    // 防止 loadDir 里的后台线程回调覆盖已被切走的目录
    @Volatile private var loadToken = 0

    // SAF 多选文件
    private val pickFilesLauncher = registerForActivityResult(
        ActivityResultContracts.OpenMultipleDocuments()
    ) { uris ->
        if (uris.isNullOrEmpty()) return@registerForActivityResult
        uploadFiles(uris)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityFileManagerBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // 根目录固定为共享盘目录
        rootDir = ShareDirManager.getDefaultShareDir(this)
        currentDir = rootDir

        // Toolbar 返回
        binding.toolbar.setNavigationOnClickListener { handleBack() }

        // RecyclerView
        adapter = FileAdapter(
            items = mutableListOf(),
            onClick = { file ->
                if (file.isDirectory) {
                    currentDir = file
                    loadDir()
                } else {
                    showFileActions(file)
                }
            },
            onLongClick = { file -> confirmDelete(file) }
        )
        binding.rvFiles.layoutManager = LinearLayoutManager(this)
        binding.rvFiles.adapter = adapter

        // 底部按钮
        binding.btnUpload.setOnClickListener {
            pickFilesLauncher.launch(arrayOf("*/*"))
        }
        binding.btnNewFolder.setOnClickListener { showNewFolderDialog() }

        loadDir()
    }

    // ============================================================
    // 返回逻辑
    // ============================================================

    private fun handleBack() {
        val dir = currentDir
        if (dir != null && dir.absolutePath != rootDir.absolutePath) {
            currentDir = dir.parentFile ?: rootDir
            loadDir()
        } else {
            onBackPressedDispatcher.onBackPressed()
        }
    }

    @Deprecated("AndroidX 已改为 onBackPressedDispatcher 回调")
    override fun onBackPressed() {
        val dir = currentDir
        if (dir != null && dir.absolutePath != rootDir.absolutePath) {
            currentDir = dir.parentFile ?: rootDir
            loadDir()
        } else {
            @Suppress("DEPRECATION")
            super.onBackPressed()
        }
    }

    // ============================================================
    // 目录加载（后台线程读盘，避免 ANR）
    // ============================================================

    private fun loadDir() {
        val dir = currentDir ?: return

        if (!dir.exists()) {
            toast("目录不存在")
            currentDir = rootDir
            return
        }

        // 更新路径显示（很快，主线程做）
        val relative = dir.absolutePath.removePrefix(rootDir.absolutePath).ifEmpty { "/" }
        binding.tvPath.text = relative

        // 显示加载态
        binding.tvEmpty.visibility = View.GONE
        binding.rvFiles.visibility = View.VISIBLE

        // token 用于丢弃过期回调
        val token = ++loadToken

        Thread {
            val canRead = dir.canRead()
            val files = if (canRead) {
                try {
                    dir.listFiles()?.toList() ?: emptyList()
                } catch (e: Throwable) {
                    Log.w(TAG, "listFiles 失败: ${e.message}")
                    emptyList()
                }
            } else {
                emptyList()
            }

            val sorted = files.sortedWith(
                compareByDescending<File> { it.isDirectory }
                    .thenBy { it.name.lowercase() }
            )

            runOnUiThread {
                // 目录可能在读盘期间被切走 / 又触发了一次 loadDir
                if (token != loadToken) return@runOnUiThread
                if (currentDir != dir) return@runOnUiThread

                if (!canRead) {
                    toast("无法读取此目录")
                    return@runOnUiThread
                }
                if (sorted.isEmpty()) {
                    binding.tvEmpty.visibility = View.VISIBLE
                    binding.rvFiles.visibility = View.GONE
                } else {
                    binding.tvEmpty.visibility = View.GONE
                    binding.rvFiles.visibility = View.VISIBLE
                }
                adapter.update(sorted)
            }
        }.start()
    }

    // ============================================================
    // 上传
    // ============================================================

    private fun uploadFiles(uris: List<Uri>) {
        val dir = currentDir ?: return
        toast("正在上传 ${uris.size} 个文件...")

        Thread {
            var ok = 0
            var fail = 0
            val failedNames = mutableListOf<String>()

            for (uri in uris) {
                val displayName = queryFileName(uri) ?: "file_${System.currentTimeMillis()}"
                try {
                    // 避免覆盖：如果已存在，加后缀
                    var dst = File(dir, displayName)
                    var counter = 1
                    while (dst.exists()) {
                        val base = displayName.substringBeforeLast('.', displayName)
                        val ext = displayName.substringAfterLast('.', "")
                        val newName = if (ext.isEmpty()) "${base}_$counter"
                        else "${base}_$counter.$ext"
                        dst = File(dir, newName)
                        counter++
                    }

                    contentResolver.openInputStream(uri)?.use { input ->
                        dst.outputStream().use { output ->
                            input.copyTo(output)
                        }
                    } ?: throw IllegalStateException("openInputStream 返回 null")

                    ok++
                } catch (e: Exception) {
                    Log.w(TAG, "上传失败: $displayName", e)
                    failedNames.add(displayName)
                    fail++
                }
            }

            runOnUiThread {
                val msg = when {
                    fail == 0 -> "已上传 $ok 个文件"
                    ok == 0 -> "上传失败（${fail} 个）：${failedNames.joinToString(", ")}"
                    else -> "成功 $ok 个，失败 $fail 个：${failedNames.joinToString(", ")}"
                }
                toast(msg)
                loadDir()
            }
        }.start()
    }

    private fun queryFileName(uri: Uri): String? {
        var name: String? = null
        try {
            contentResolver.query(uri, null, null, null, null)?.use { c ->
                if (c.moveToFirst()) {
                    val idx = c.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                    if (idx >= 0) name = c.getString(idx)
                }
            }
        } catch (e: Throwable) {
            Log.w(TAG, "queryFileName 失败: ${e.message}")
        }
        return name
    }

    // ============================================================
    // 新建文件夹
    // ============================================================

    private fun showNewFolderDialog() {
        val input = EditText(this).apply {
            hint = "文件夹名"
            setPadding(48, 32, 48, 32)
        }
        AlertDialog.Builder(this)
            .setTitle("新建文件夹")
            .setView(input)
            .setPositiveButton("创建") { _, _ ->
                val name = input.text.toString().trim()
                if (name.isEmpty()) {
                    toast("名称不能为空")
                    return@setPositiveButton
                }
                if (name.contains("/") || name.contains("\\")) {
                    toast("名称不能包含斜杠")
                    return@setPositiveButton
                }
                if (name == "." || name == "..") {
                    toast("名称非法")
                    return@setPositiveButton
                }
                val baseDir = currentDir ?: return@setPositiveButton
                val target = File(baseDir, name)
                if (target.exists()) {
                    toast("已存在同名文件夹")
                    return@setPositiveButton
                }
                if (target.mkdirs()) {
                    toast("已创建")
                    loadDir()
                } else {
                    toast("创建失败")
                }
            }
            .setNegativeButton("取消", null)
            .show()
    }

    // ============================================================
    // 删除
    // ============================================================

    private fun confirmDelete(file: File) {
        val type = if (file.isDirectory) "文件夹" else "文件"
        AlertDialog.Builder(this)
            .setTitle("删除$type")
            .setMessage("确定删除 \"${file.name}\" 吗？\n\n此操作不可撤销。")
            .setPositiveButton("删除") { _, _ ->
                val ok = deleteRecursively(file)
                toast(if (ok) "已删除" else "删除失败")
                loadDir()
            }
            .setNegativeButton("取消", null)
            .show()
    }

    private fun deleteRecursively(file: File): Boolean {
        return try {
            if (file.isDirectory) {
                file.listFiles()?.forEach { deleteRecursively(it) }
            }
            file.delete()
        } catch (e: Exception) {
            false
        }
    }

    // ============================================================
    // 文件操作菜单
    // ============================================================

    private fun showFileActions(file: File) {
        val options = arrayOf("📤 分享", "👁 预览", "💾 导出到 Download", "🗑 删除")
        AlertDialog.Builder(this)
            .setTitle(file.name)
            .setItems(options) { _, which ->
                when (which) {
                    0 -> shareFile(file)
                    1 -> previewFile(file)
                    2 -> exportToDownload(file)
                    3 -> confirmDelete(file)
                }
            }
            .show()
    }

    // ============================================================
    // 分享
    // ============================================================

    private fun shareFile(file: File) {
        try {
            val uri: Uri = FileProvider.getUriForFile(
                this,
                "com.n2n.android.fileprovider",
                file
            )
            val intent = Intent(Intent.ACTION_SEND).apply {
                type = getMimeType(file.name)
                putExtra(Intent.EXTRA_STREAM, uri)
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(Intent.createChooser(intent, "分享到"))
        } catch (e: Exception) {
            toast("分享失败: ${e.message}")
        }
    }

    // ============================================================
    // 预览（用系统应用打开）
    // ============================================================

    private fun previewFile(file: File) {
        try {
            val uri: Uri = FileProvider.getUriForFile(
                this,
                "com.n2n.android.fileprovider",
                file
            )
            val intent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri, getMimeType(file.name))
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(intent)
        } catch (e: Exception) {
            toast("没有能打开 ${file.name} 的应用")
        }
    }

    // ============================================================
    // 导出到 Download/n2n-share/
    // ============================================================

    private fun exportToDownload(file: File) {
        Thread {
            val ok = try {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    exportViaMediaStore(file)
                } else {
                    exportDirectLegacy(file)
                }
            } catch (e: Exception) {
                Log.w(TAG, "导出失败: ${file.name}", e)
                false
            }

            runOnUiThread {
                toast(if (ok) "已导出到 Download/n2n-share/" else "导出失败")
            }
        }.start()
    }

    @RequiresApi(Build.VERSION_CODES.Q)
    private fun exportViaMediaStore(file: File): Boolean {
        val values = ContentValues().apply {
            put(MediaStore.MediaColumns.DISPLAY_NAME, file.name)
            put(MediaStore.MediaColumns.MIME_TYPE, getMimeType(file.name))
            put(MediaStore.MediaColumns.RELATIVE_PATH, "Download/n2n-share")
        }
        val uri = contentResolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
            ?: return false
        return try {
            contentResolver.openOutputStream(uri)?.use { out ->
                file.inputStream().use { it.copyTo(out) }
            } != null
        } catch (e: Exception) {
            // 清理半成品
            try { contentResolver.delete(uri, null, null) } catch (_: Exception) {}
            false
        }
    }

    @Suppress("DEPRECATION")
    private fun exportDirectLegacy(file: File): Boolean {
        val dir = File(
            Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS),
            "n2n-share"
        )
        if (!dir.exists()) dir.mkdirs()
        val dst = File(dir, file.name)
        file.inputStream().use { input ->
            dst.outputStream().use { output ->
                input.copyTo(output)
            }
        }
        return true
    }

    // ============================================================
    // MIME 类型
    // ============================================================

    private fun getMimeType(name: String): String {
        return when (name.substringAfterLast('.', "").lowercase()) {
            "jpg", "jpeg" -> "image/jpeg"
            "png" -> "image/png"
            "gif" -> "image/gif"
            "webp" -> "image/webp"
            "bmp" -> "image/bmp"
            "svg" -> "image/svg+xml"
            "heic", "heif" -> "image/heif"
            "mp4" -> "video/mp4"
            "mkv" -> "video/x-matroska"
            "mov" -> "video/quicktime"
            "avi" -> "video/x-msvideo"
            "webm" -> "video/webm"
            "3gp" -> "video/3gpp"
            "flv" -> "video/x-flv"
            "wmv" -> "video/x-ms-wmv"
            "mp3" -> "audio/mpeg"
            "wav" -> "audio/wav"
            "flac" -> "audio/flac"
            "ogg" -> "audio/ogg"
            "m4a" -> "audio/mp4"
            "aac" -> "audio/aac"
            "wma" -> "audio/x-ms-wma"
            "pdf" -> "application/pdf"
            "zip" -> "application/zip"
            "rar" -> "application/x-rar-compressed"
            "7z" -> "application/x-7z-compressed"
            "tar" -> "application/x-tar"
            "gz" -> "application/gzip"
            "bz2" -> "application/x-bzip2"
            "xz" -> "application/x-xz"
            "apk" -> "application/vnd.android.package-archive"
            "txt", "md", "log" -> "text/plain"
            "html", "htm" -> "text/html"
            "css" -> "text/css"
            "js" -> "application/javascript"
            "ts" -> "application/typescript"
            "json" -> "application/json"
            "xml" -> "application/xml"
            "csv" -> "text/csv"
            "doc", "docx" -> "application/msword"
            "xls", "xlsx" -> "application/vnd.ms-excel"
            "ppt", "pptx" -> "application/vnd.ms-powerpoint"
            else -> "*/*"
        }
    }

    // ============================================================
    // 工具
    // ============================================================

    private fun toast(msg: String) {
        Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
    }
}
