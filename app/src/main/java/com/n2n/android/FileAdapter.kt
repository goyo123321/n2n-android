package com.n2n.android

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import com.n2n.mobile.databinding.ItemFileBinding
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class FileAdapter(
    private val items: MutableList<File>,
    private val onClick: (File) -> Unit,
    private val onLongClick: (File) -> Unit,
) : RecyclerView.Adapter<FileAdapter.VH>() {

    private val dateFmt = SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.getDefault())

    inner class VH(val binding: ItemFileBinding) : RecyclerView.ViewHolder(binding.root)

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): VH {
        val binding = ItemFileBinding.inflate(
            LayoutInflater.from(parent.context),
            parent,
            false
        )
        return VH(binding)
    }

    override fun onBindViewHolder(holder: VH, position: Int) {
        val f = items[position]

        holder.binding.tvName.text = f.name
        holder.binding.tvIcon.text = if (f.isDirectory) "📁" else iconFor(f.name)

        holder.binding.tvMeta.text = if (f.isDirectory) {
            val childCount = f.listFiles()?.size ?: 0
            "文件夹 · $childCount 项 · ${dateFmt.format(Date(f.lastModified()))}"
        } else {
            "${fmtSize(f.length())} · ${dateFmt.format(Date(f.lastModified()))}"
        }

        holder.binding.root.setOnClickListener { onClick(f) }
        holder.binding.root.setOnLongClickListener {
            onLongClick(f)
            true
        }
    }

    override fun getItemCount() = items.size

    fun update(newItems: List<File>) {
        items.clear()
        items.addAll(newItems)
        notifyDataSetChanged()
    }

    // ============================================================
    // 文件类型图标
    // ============================================================

    private fun iconFor(name: String): String {
        return when (name.substringAfterLast('.', "").lowercase()) {
            // 图片
            "jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "heic", "heif" -> "🖼"
            // 视频
            "mp4", "mkv", "mov", "avi", "webm", "3gp", "flv", "wmv" -> "🎬"
            // 音频
            "mp3", "wav", "flac", "ogg", "m4a", "aac", "wma" -> "🎵"
            // 文档
            "pdf" -> "📕"
            "doc", "docx" -> "📘"
            "xls", "xlsx", "csv" -> "📗"
            "ppt", "pptx" -> "📙"
            "txt", "md", "log" -> "📝"
            // 压缩包
            "zip", "rar", "7z", "tar", "gz", "bz2", "xz" -> "📦"
            // 安装包
            "apk" -> "🤖"
            // 代码
            "html", "htm", "css", "js", "ts", "json", "xml" -> "📄"
            "go", "kt", "java", "py", "rs", "c", "cpp", "h" -> "📄"
            else -> "📄"
        }
    }

    // ============================================================
    // 文件大小格式化
    // ============================================================

    private fun fmtSize(b: Long): String {
        if (b < 1024) return "$b B"
        if (b < 1048576) return "%.1f KB".format(b / 1024.0)
        if (b < 1073741824) return "%.1f MB".format(b / 1048576.0)
        return "%.2f GB".format(b / 1073741824.0)
    }
}
