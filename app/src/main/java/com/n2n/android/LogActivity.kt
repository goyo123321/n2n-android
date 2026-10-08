package com.n2n.android

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.n2n.android.databinding.ActivityLogBinding

class LogActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "LogActivity"
    }

    private lateinit var binding: ActivityLogBinding

    private val ticker = object : Runnable {
        override fun run() {
            if (isFinishing || isDestroyed || !::binding.isInitialized) return
            refresh()
            binding.root.postDelayed(this, 1000)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityLogBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.toolbar.setNavigationOnClickListener {
            onBackPressedDispatcher.onBackPressed()
        }

        binding.toolbar.inflateMenu(R.menu.menu_logs)
        binding.toolbar.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                R.id.action_clear_logs -> {
                    N2nController.clearLogs()
                    refresh()
                    toast(getString(R.string.log_cleared))
                    true
                }
                R.id.action_copy_logs -> {
                    val logs = N2nController.getLogs()
                    if (logs.isEmpty()) {
                        toast(getString(R.string.log_empty_copy))
                    } else {
                        try {
                            val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                            cm.setPrimaryClip(ClipData.newPlainText("n2n logs", logs))
                            toast(getString(R.string.log_copied))
                        } catch (t: Throwable) {
                            Log.e(TAG, "copy failed", t)
                        }
                    }
                    true
                }
                else -> false
            }
        }

        binding.switchAutoScroll.setOnCheckedChangeListener { _, checked ->
            if (!::binding.isInitialized) return@setOnCheckedChangeListener
            binding.tvAutoScroll.text = getString(
                if (checked) R.string.log_autoscroll_on else R.string.log_autoscroll_off
            )
        }

        refresh()
    }

    override fun onStart() {
        super.onStart()
        if (!::binding.isInitialized) return
        binding.root.removeCallbacks(ticker)
        binding.root.postDelayed(ticker, 1000)
    }

    override fun onStop() {
        super.onStop()
        if (!::binding.isInitialized) return
        binding.root.removeCallbacks(ticker)
    }

    override fun onDestroy() {
        if (::binding.isInitialized) {
            binding.root.removeCallbacks(ticker)
        }
        super.onDestroy()
    }

    private fun refresh() {
        if (isFinishing || isDestroyed || !::binding.isInitialized) return
        try {
            val logs = N2nController.getLogs()
            val text = if (logs.isEmpty()) getString(R.string.log_empty) else logs
            if (binding.tvLogs.text.toString() != text) {
                binding.tvLogs.text = text
                if (binding.switchAutoScroll.isChecked) {
                    binding.scrollView.post {
                        if (!isFinishing && !isDestroyed && ::binding.isInitialized) {
                            binding.scrollView.fullScroll(View.FOCUS_DOWN)
                        }
                    }
                }
            }
        } catch (t: Throwable) {
            Log.e(TAG, "refresh failed", t)
        }
    }

    private fun toast(msg: String) {
        if (isFinishing || isDestroyed) return
        try {
            Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
        } catch (_: Throwable) {}
    }
}
