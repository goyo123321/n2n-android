package com.n2n.android

import android.content.ContentValues
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.provider.MediaStore
import android.provider.OpenableColumns
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

    private lateinit var binding: ActivityFileManagerBinding
    private lateinit var adapter: FileAdapter
    private lateinit var rootDir: File
    private var currentDir: File? = null

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

        // 根目录
        rootDir = ShareDirManager.getDefaultShareDir(this)
        currentDir = rootDir

        // Toolbar 返回
        binding.toolbar.setNavigationOnClickListener {
            if (currentDir?.absolutePath != rootDir.absolutePath) {
                currentDir = currentDir?.parentFile ?: rootDir
                loadDir()
            } else {
                onBackPressedDispatcher.onBackPressed()
            }
        }

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
    // 目录加载
    // ============================================================

    private fun loadDir() {
        val dir = currentDir ?: return

        if (!dir.exists()) {
            toast("目录不存在")
            currentDir = rootDir
            return
        }
        if (!dir.canRead()) {
            toast("无法读取此目录")
            return
        }

        // 更新路径显示
        val relative = dir.absolutePath.removePrefix(rootDir.absolutePath).ifEmpty { "/" }
        binding.tvPath.text = relative

        // 列出文件：文件夹优先，然后按名称排序
        val files = dir.listFiles()?.toList() ?: emptyList()
        val sorted = files.sortedWith(
            compareByDescending<File> { it.isDirectory }
                .thenBy { it.name.lowercase() }
        )

        if (sorted.isEmpty()) {
            binding.tvEmpty.visibility = android.view.View.VISIBLE
            binding.rvFiles.visibility = android.view.View.GONE
        } else {
            binding.tvEmpty.visibility = android.view.View.GONE
            binding.rvFiles.visibility = android.view.View.VISIBLE
        }
        adapter.update(sorted)
    }

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
    // 上传
    // ============================================================

    private fun uploadFiles(uris: List<Uri>) {
        val dir = currentDir ?: return
        toast("正在上传 ${uris.size} 个文件...")

        Thread {
            var ok = 0
            var fail = 0
            for (uri in uris) {
                try {
                    val name = queryFileName(uri) ?: "file_${System.currentTimeMillis()}"

                    // 避免覆盖：如果已存在，加后缀
                    var dst = File(dir, name)
                    var counter = 1
                    while (dst.exists()) {
                        val base = name.substringBeforeLast('.', name)
                        val ext = name.substringAfterLast('.', "")
                        val newName = if (ext.isEmpty()) "${base}_$counter"
                        else "${base}_$counter.$ext"
                        dst = File(dir, newName)
                        counter++
                    }

                    contentResolver.openInputStream(uri)?.use { input ->
                        dst.outputStream().use { output ->
                            input.copyTo(output)
                        }
                    } ?: throw IllegalStateException("openInputStream null")

                    ok++
                } catch (e: Exception) {
                    fail++
                }
            }

            runOnUiThread {
                val msg = when {
                    fail == 0 -> "已上传 $ok 个文件"
                    ok == 0 -> "上传失败（$fail 个）"
                    else -> "上传完成：成功 $ok，失败 $fail"
                }
                toast(msg)
                loadDir()
            }
        }.start()
    }

    private fun queryFileName(uri: Uri): String? {
        var name: String? = null
        contentResolver.query(uri, null, null, null, null)?.use { c ->
            if (c.moveToFirst()) {
                val idx = c.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                if (idx >= 0) name = c.getString(idx)
            }
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
                val target = File(currentDir, name)
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
        contentResolver.openOutputStream(uri)?.use { out ->
            file.inputStream().use { it.copyTo(out) }
        } ?: return false
        return true
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
            "mp4" -> "video/mp4"
            "mkv" -> "video/x-matroska"
            "mov" -> "video/quicktime"
            "avi" -> "video/x-msvideo"
            "webm" -> "video/webm"
            "3gp" -> "video/3gpp"
            "mp3" -> "audio/mpeg"
            "wav" -> "audio/wav"
            "flac" -> "audio/flac"
            "ogg" -> "audio/ogg"
            "m4a" -> "audio/mp4"
            "aac" -> "audio/aac"
            "pdf" -> "application/pdf"
            "zip" -> "application/zip"
            "rar" -> "application/x-rar-compressed"
            "7z" -> "application/x-7z-compressed"
            "tar" -> "application/x-tar"
            "gz" -> "application/gzip"
            "apk" -> "application/vnd.android.package-archive"
            "txt", "md", "log" -> "text/plain"
            "html", "htm" -> "text/html"
            "css" -> "text/css"
            "js" -> "application/javascript"
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
