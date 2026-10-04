package com.n2n.android

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.n2n.android.databinding.ActivityLogBinding

class LogActivity : AppCompatActivity() {

    private lateinit var binding: ActivityLogBinding

    private val ticker = object : Runnable {
        override fun run() {
            refresh()
            binding.root.postDelayed(this, 1000)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityLogBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // Toolbar 返回
        binding.toolbar.setNavigationOnClickListener {
            onBackPressedDispatcher.onBackPressed()
        }

        // 菜单
        binding.toolbar.inflateMenu(R.menu.menu_logs)
        binding.toolbar.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                R.id.action_clear_logs -> {
                    N2nController.clearLogs()
                    refresh()
                    toast("已清空")
                    true
                }
                R.id.action_copy_logs -> {
                    val logs = N2nController.getLogs()
                    if (logs.isEmpty()) {
                        toast("暂无日志可复制")
                    } else {
                        val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                        cm.setPrimaryClip(ClipData.newPlainText("n2n logs", logs))
                        toast("已复制到剪贴板")
                    }
                    true
                }
                else -> false
            }
        }

        // 自动滚动开关
        binding.switchAutoScroll.setOnCheckedChangeListener { _, checked ->
            binding.tvAutoScroll.text = if (checked) "🔄 自动滚动已开启" else "⏸ 自动滚动已关闭"
        }

        refresh()
        binding.root.postDelayed(ticker, 1000)
    }

    override fun onDestroy() {
        super.onDestroy()
        binding.root.removeCallbacks(ticker)
    }

    private fun refresh() {
        val logs = N2nController.getLogs()
        val text = if (logs.isEmpty()) "(暂无日志)" else logs
        if (binding.tvLogs.text.toString() != text) {
            binding.tvLogs.text = text
            if (binding.switchAutoScroll.isChecked) {
                binding.scrollView.post {
                    binding.scrollView.fullScroll(View.FOCUS_DOWN)
                }
            }
        }
    }

    private fun toast(msg: String) {
        Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
    }
}
