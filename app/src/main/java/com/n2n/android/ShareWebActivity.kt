package com.n2n.android

import android.annotation.SuppressLint
import android.os.Bundle
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AppCompatActivity
import com.n2n.android.databinding.ActivityShareWebBinding

class ShareWebActivity : AppCompatActivity() {

    companion object {
        const val EXTRA_VIP = "vip"
        const val EXTRA_PORT = "port"
        const val EXTRA_TITLE = "title"
    }

    private lateinit var binding: ActivityShareWebBinding

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityShareWebBinding.inflate(layoutInflater)
        setContentView(binding.root)

        val vip = intent.getStringExtra(EXTRA_VIP) ?: run { finish(); return }
        val port = intent.getIntExtra(EXTRA_PORT, 9090)
        val title = intent.getStringExtra(EXTRA_TITLE) ?: vip

        binding.toolbar.title = title
        binding.toolbar.subtitle = "http://$vip:$port/"
        binding.toolbar.setNavigationOnClickListener {
            onBackPressedDispatcher.onBackPressed()
        }

        val web = binding.webView
        web.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            loadWithOverviewMode = true
            useWideViewPort = true
            builtInZoomControls = true
            displayZoomControls = false
            cacheMode = WebSettings.LOAD_NO_CACHE
            mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        }
        web.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                val url = request?.url?.toString() ?: return false
                return !url.startsWith("http://$vip:$port/")
            }
        }
        web.loadUrl("http://$vip:$port/")
    }

    override fun onDestroy() {
        binding.webView.destroy()
        super.onDestroy()
    }
}
