package com.n2n.android

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.net.VpnService
import android.os.Build
import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.inputmethod.InputMethodManager
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.app.AppCompatDelegate
import com.n2n.android.databinding.ActivityMainBinding
import com.n2n.android.databinding.ItemPeerBinding
import org.json.JSONArray

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding

    private val peersTicker = object : Runnable {
        override fun run() {
            if (N2nController.isRunning()) refreshPeers()
            binding.root.postDelayed(this, 3000)
        }
    }

    private val statusTicker = object : Runnable {
        override fun run() {
            refreshStatus()
            binding.root.postDelayed(this, 1000)
        }
    }

    private val vpnPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { result ->
        if (result.resultCode == Activity.RESULT_OK) startVpnService()
        else toast("用户拒绝 VPN 权限")
    }

    private val notifPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { }

    private val pickDirLauncher = registerForActivityResult(
        ActivityResultContracts.OpenDocumentTree()
    ) { uri: Uri? ->
        if (uri != null) {
            try {
                contentResolver.takePersistableUriPermission(
                    uri,
                    Intent.FLAG_GRANT_READ_URI_PERMISSION or
                        Intent.FLAG_GRANT_WRITE_URI_PERMISSION
                )
                Prefs.saveShareDirUri(this, uri.toString())
                refreshShareDirDisplay(uri.toString())
                toast("共享目录已更新")
            } catch (e: Exception) {
                toast("授权失败: ${e.message}")
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        applyThemeMode(Prefs.loadThemeMode(this))
        super.onCreate(savedInstanceState)

        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        loadConfig(savedInstanceState)
        handleIntentParams(intent)
        refreshShareDirDisplay(Prefs.loadShareDirUri(this))

        requestNotifPermissionIfNeeded()
        requestIgnoreBatteryOptimizationIfNeeded()
        showFirstLaunchDialogIfNeeded()
        checkSignalingUrl()

        binding.toolbar.inflateMenu(R.menu.menu_main)
        binding.toolbar.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                R.id.action_logs -> {
                    startActivity(Intent(this, LogActivity::class.java))
                    true
                }
                R.id.action_theme -> {
                    showThemePicker()
                    true
                }
                else -> false
            }
        }

        binding.btnStart.setOnClickListener { saveThenRequest() }
        binding.btnStop.setOnClickListener { stopVpnService() }
        binding.btnSave.setOnClickListener {
            saveCurrentInput()
            toast("已保存")
        }
        binding.btnPickDir.setOnClickListener { pickDirLauncher.launch(null) }
        binding.btnManageFiles.setOnClickListener {
            startActivity(Intent(this, FileManagerActivity::class.java))
        }
        binding.btnOpenMyShare.setOnClickListener { openMyShare() }

        refreshStatus()
        binding.root.postDelayed(peersTicker, 3000)
        binding.root.postDelayed(statusTicker, 1000)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleIntentParams(intent)
    }

    override fun onResume() {
        super.onResume()
        refreshStatus()
        if (N2nController.isRunning()) refreshPeers()
    }

    override fun onPause() {
        super.onPause()
        saveCurrentInput()
    }

    override fun onDestroy() {
        super.onDestroy()
        binding.root.removeCallbacks(peersTicker)
        binding.root.removeCallbacks(statusTicker)
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        outState.putString("signalingUrl", binding.etSignalingUrl.text.toString())
        outState.putString("preferredIp", binding.etPreferredIp.text.toString())
        outState.putString("roomId", binding.etRoomId.text.toString())
        outState.putString("clientId", binding.etClientId.text.toString())
        outState.putString("nodeName", binding.etNodeName.text.toString())
        outState.putString("connectToken", binding.etConnectToken.text.toString())
    }

    // ============================================================
    // 工具：URL host 格式化（IPv6 加方括号）
    // ============================================================

    private fun formatHost(host: String): String {
        if (host.contains(":") && !host.startsWith("[")) {
            return "[$host]"
        }
        return host
    }

    // ============================================================
    // 主题
    // ============================================================

    private fun applyThemeMode(mode: Int) {
        val nightMode = when (mode) {
            1 -> AppCompatDelegate.MODE_NIGHT_NO
            2 -> AppCompatDelegate.MODE_NIGHT_YES
            else -> AppCompatDelegate.MODE_NIGHT_FOLLOW_SYSTEM
        }
        AppCompatDelegate.setDefaultNightMode(nightMode)
    }

    private fun showThemePicker() {
        val current = Prefs.loadThemeMode(this)
        val options = arrayOf("跟随系统", "浅色", "深色")
        AlertDialog.Builder(this)
            .setTitle("主题")
            .setSingleChoiceItems(options, current) { dialog, which ->
                Prefs.saveThemeMode(this, which)
                applyThemeMode(which)
                dialog.dismiss()
            }
            .show()
    }

    // ============================================================
    // 权限
    // ============================================================

    private fun requestNotifPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            notifPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
        }
    }

    @android.annotation.SuppressLint("BatteryLife")
    private fun requestIgnoreBatteryOptimizationIfNeeded() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return
        val pm = getSystemService(android.content.Context.POWER_SERVICE)
            as android.os.PowerManager
        val pkg = packageName
        if (pm.isIgnoringBatteryOptimizations(pkg)) return

        try {
            val intent = Intent(
                android.provider.Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS
            ).apply {
                data = android.net.Uri.parse("package:$pkg")
            }
            startActivity(intent)
        } catch (e: Exception) {
            try {
                startActivity(Intent(android.provider.Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS))
            } catch (_: Exception) {}
        }
    }

    // ============================================================
    // 首次启动
    // ============================================================

    private fun showFirstLaunchDialogIfNeeded() {
        if (!Prefs.isFirstLaunch(this)) return
        AlertDialog.Builder(this)
            .setTitle("共享目录位置")
            .setMessage(
                """
                你的共享目录位于：
                Android/media/com.n2n.android/shared/

                • MT 管理器可直接访问（无需授权）
                • USB 连电脑可直接拖拽文件
                • 微信/QQ 能直接选到里面的文件
                • PC 端可通过 n2n 组网访问

                系统首次访问时会提示"允许访问媒体文件"，请点"允许"以启用外部访问。
                """.trimIndent()
            )
            .setPositiveButton("知道了") { _, _ ->
                Prefs.setFirstLaunchDone(this)
            }
            .setCancelable(false)
            .show()
    }

    private fun checkSignalingUrl() {
        val currentUrl = binding.etSignalingUrl.text.toString().trim()
        if (currentUrl.isNotEmpty()) return

        AlertDialog.Builder(this)
            .setTitle("首次使用：请填写服务器地址")
            .setMessage(
                """
                你需要填写 edge-signal Worker 的 WSS 地址。

                如果你已经部署了 edge-signal：
                  • 打开 Cloudflare Dashboard
                  • Workers & Pages → edge-signal
                  • 复制访问地址（形如 wss://xxx.workers.dev）

                如果你还没有部署：
                  • 参考 edge-signal 项目文档
                  • 或使用朋友分享给你的地址
                """.trimIndent()
            )
            .setPositiveButton("我现在就填") { _, _ ->
                binding.etSignalingUrl.requestFocus()
                binding.etSignalingUrl.setSelection(
                    binding.etSignalingUrl.text?.length ?: 0
                )
                val imm = getSystemService(INPUT_METHOD_SERVICE) as InputMethodManager
                imm.showSoftInput(
                    binding.etSignalingUrl,
                    InputMethodManager.SHOW_IMPLICIT
                )
            }
            .setNegativeButton("稍后再说", null)
            .setCancelable(false)
            .show()
    }

    // ============================================================
    // 配置加载 / 保存
    // ============================================================

    private fun loadConfig(savedInstanceState: Bundle?) {
        if (savedInstanceState != null) {
            binding.etSignalingUrl.setText(savedInstanceState.getString("signalingUrl", ""))
            binding.etPreferredIp.setText(savedInstanceState.getString("preferredIp", ""))
            binding.etRoomId.setText(savedInstanceState.getString("roomId", ""))
            binding.etClientId.setText(savedInstanceState.getString("clientId", ""))
            binding.etNodeName.setText(savedInstanceState.getString("nodeName", ""))
            binding.etConnectToken.setText(savedInstanceState.getString("connectToken", ""))
            return
        }
        val cfg = Prefs.load(this)
        binding.etSignalingUrl.setText(cfg.signalingUrl)
        binding.etPreferredIp.setText(Prefs.loadPreferredIp(this))
        binding.etRoomId.setText(cfg.roomId)
        binding.etClientId.setText(cfg.clientId)
        binding.etNodeName.setText(cfg.nodeName)
        binding.etConnectToken.setText(cfg.connectToken)
    }

    private fun handleIntentParams(intent: Intent?) {
        intent ?: return

        if (intent.action == Intent.ACTION_VIEW && intent.data != null) {
            val data = intent.data!!
            if (data.scheme == "n2n" && data.host == "connect") {
                data.getQueryParameter("url")?.let { binding.etSignalingUrl.setText(it) }
                data.getQueryParameter("ip")?.let { binding.etPreferredIp.setText(it) }
                data.getQueryParameter("room")?.let { binding.etRoomId.setText(it) }
                data.getQueryParameter("cid")?.let { binding.etClientId.setText(it) }
                data.getQueryParameter("name")?.let { binding.etNodeName.setText(it) }
                data.getQueryParameter("token")?.let { binding.etConnectToken.setText(it) }
                saveCurrentInput()
                if (data.getQueryParameter("auto") == "1") {
                    binding.root.postDelayed({ requestVpnPermission() }, 300)
                }
                return
            }
        }

        val url = intent.getStringExtra(N2nVpnService.EXTRA_SIGNALING_URL)
        val ip = intent.getStringExtra("preferred_ip")
        val room = intent.getStringExtra(N2nVpnService.EXTRA_ROOM_ID)
        val cid = intent.getStringExtra(N2nVpnService.EXTRA_CLIENT_ID)
        val name = intent.getStringExtra(N2nVpnService.EXTRA_NODE_NAME)
        val token = intent.getStringExtra(N2nVpnService.EXTRA_CONNECT_TOKEN)

        var changed = false
        if (!url.isNullOrEmpty()) { binding.etSignalingUrl.setText(url); changed = true }
        if (!ip.isNullOrEmpty()) { binding.etPreferredIp.setText(ip); changed = true }
        if (!room.isNullOrEmpty()) { binding.etRoomId.setText(room); changed = true }
        if (!cid.isNullOrEmpty()) { binding.etClientId.setText(cid); changed = true }
        if (!name.isNullOrEmpty()) { binding.etNodeName.setText(name); changed = true }
        if (!token.isNullOrEmpty()) { binding.etConnectToken.setText(token); changed = true }

        if (changed) {
            saveCurrentInput()
            toast("已从外部参数更新配置")
        }
        if (intent.getBooleanExtra("auto_start", false)) {
            binding.root.postDelayed({ requestVpnPermission() }, 300)
        }
    }

    private fun saveCurrentInput() {
        if (binding.etClientId.text.toString().trim().isEmpty()) {
            binding.etClientId.setText(Prefs.loadOrCreateClientId(this))
        }
        Prefs.save(this, Prefs.Config(
            signalingUrl = binding.etSignalingUrl.text.toString().trim(),
            roomId = binding.etRoomId.text.toString().trim(),
            clientId = binding.etClientId.text.toString().trim(),
            nodeName = binding.etNodeName.text.toString().trim(),
            connectToken = binding.etConnectToken.text.toString().trim(),
        ))
        Prefs.savePreferredIp(this, binding.etPreferredIp.text.toString().trim())
    }

    // ============================================================
    // 启动 / 停止
    // ============================================================

    private fun saveThenRequest() {
        saveCurrentInput()
        requestVpnPermission()
    }

    private fun requestVpnPermission() {
        val url = binding.etSignalingUrl.text.toString().trim()

        if (url.isEmpty()) {
            toast("请先填写 WSS 地址")
            checkSignalingUrl()
            return
        }
        if (!url.startsWith("wss://") && !url.startsWith("ws://")) {
            toast("WSS 地址必须以 wss:// 或 ws:// 开头")
            return
        }

        val room = binding.etRoomId.text.toString().trim()
        if (room.isEmpty()) {
            toast("房间名不能为空")
            return
        }
        if (!room.matches(Regex("^[A-Za-z0-9_-]+$"))) {
            toast("房间名只允许字母/数字/下划线/短横线")
            return
        }

        val intent = VpnService.prepare(this)
        if (intent != null) {
            vpnPermissionLauncher.launch(intent)
        } else {
            startVpnService()
        }
    }

    private fun startVpnService() {
        val safUri = Prefs.loadShareDirUri(this)
        val shareDir = ShareDirManager.getAbsolutePathForGo(this, safUri)
        val preferredIp = binding.etPreferredIp.text.toString().trim()

        val intent = Intent(this, N2nVpnService::class.java).apply {
            action = N2nVpnService.ACTION_START
            putExtra(N2nVpnService.EXTRA_SIGNALING_URL, binding.etSignalingUrl.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_PREFERRED_IP, preferredIp)
            putExtra(N2nVpnService.EXTRA_ROOM_ID, binding.etRoomId.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_CLIENT_ID, binding.etClientId.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_NODE_NAME, binding.etNodeName.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_CONNECT_TOKEN, binding.etConnectToken.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_SHARE_DIR, shareDir)
        }
        startForegroundService(intent)
        toast("正在启动...")
        binding.root.postDelayed({ refreshStatus() }, 500)
    }

    private fun stopVpnService() {
        val intent = Intent(this, N2nVpnService::class.java).apply {
            action = N2nVpnService.ACTION_STOP
        }
        startService(intent)
        toast("已停止")
        binding.root.postDelayed({ refreshStatus() }, 500)
    }

    // ============================================================
    // 状态刷新
    // ============================================================

    private fun refreshStatus() {
        val running = N2nController.isRunning()

        if (running) {
            binding.tvStatus.text = getString(R.string.status_running)
            binding.tvStatus.setTextColor(getColor(R.color.brand_success))
            binding.tvVirtualIp.text = "虚拟 IP: ${N2nController.getVirtualIP()}"
            binding.tvClientId.text = "Client ID: ${N2nController.getClientID()}"
            binding.tvVirtualIp.visibility = View.VISIBLE
            binding.tvClientId.visibility = View.VISIBLE
            binding.cardStatus.setStrokeColor(getColor(R.color.brand_success))

            val vip = N2nController.getVirtualIP()
            if (vip.isNotEmpty()) {
                val host = formatHost(vip)   // ★ IPv6 加方括号
                binding.tvShareUrl.text = "共享盘: http://$host:9090/"
                binding.tvShareUrl.visibility = View.VISIBLE

                binding.tvShareVip.text = "http://$host:9090/"
                binding.tvShareVip.visibility = View.VISIBLE
                binding.btnOpenMyShare.isEnabled = true
                binding.btnOpenMyShare.alpha = 1.0f
            } else {
                binding.tvShareUrl.visibility = View.GONE
                binding.tvShareVip.visibility = View.GONE
                binding.btnOpenMyShare.isEnabled = false
                binding.btnOpenMyShare.alpha = 0.5f
            }

            binding.btnStart.visibility = View.GONE
            binding.btnStop.visibility = View.VISIBLE
        } else {
            binding.tvStatus.text = getString(R.string.status_stopped)
            binding.tvStatus.setTextColor(getColor(R.color.brand_text_dim))
            binding.tvVirtualIp.visibility = View.GONE
            binding.tvClientId.visibility = View.GONE
            binding.tvShareUrl.visibility = View.GONE

            binding.tvShareVip.visibility = View.GONE
            binding.btnOpenMyShare.isEnabled = false
            binding.btnOpenMyShare.alpha = 0.5f

            binding.cardStatus.setStrokeColor(getColor(R.color.brand_border))

            binding.btnStart.visibility = View.VISIBLE
            binding.btnStop.visibility = View.GONE
        }
    }

    // ============================================================
    // 节点列表
    // ============================================================

    private fun refreshPeers() {
        if (!N2nController.isRunning()) {
            binding.tvPeerCount.text = "0"
            binding.tvPeersEmpty.visibility = View.VISIBLE
            binding.peersContainer.removeAllViews()
            return
        }
        val json = N2nController.getPeersJSON()
        val peers = try { JSONArray(json) } catch (e: Exception) { JSONArray() }

        binding.tvPeerCount.text = peers.length().toString()

        if (peers.length() == 0) {
            binding.tvPeersEmpty.visibility = View.VISIBLE
            binding.peersContainer.removeAllViews()
            return
        }
        binding.tvPeersEmpty.visibility = View.GONE
        binding.peersContainer.removeAllViews()
        val inflater = LayoutInflater.from(this)

        for (i in 0 until peers.length()) {
            val p = peers.getJSONObject(i)
            val item = ItemPeerBinding.inflate(inflater, binding.peersContainer, false)
            item.tvPeerCode.text = p.optString("code", "?")
            item.tvPeerVip.text = p.optString("vip", "--")

            val connType = p.optString("connType", "unknown")
            val natType = p.optString("natType", "unknown")
            item.tvPeerMeta.text = "$connType · $natType"

            val vip = p.optString("vip", "")
            val sharePort = p.optInt("sharePort", 9090)

            item.btnOpenShare.isEnabled = vip.isNotEmpty()
            item.btnOpenShare.setOnClickListener {
                openShareInBrowser(vip, sharePort)
            }
            binding.peersContainer.addView(item.root)
        }
    }

    private fun openMyShare() {
        if (!N2nController.isRunning()) {
            toast("请先启动 VPN")
            return
        }
        val vip = N2nController.getVirtualIP()
        if (vip.isEmpty()) {
            toast("虚拟 IP 未分配，稍后再试")
            return
        }
        val intent = Intent(this, ShareWebActivity::class.java).apply {
            putExtra(ShareWebActivity.EXTRA_VIP, vip)
            putExtra(ShareWebActivity.EXTRA_PORT, 9090)
            putExtra(ShareWebActivity.EXTRA_TITLE, "本机共享盘")
        }
        startActivity(intent)
    }

    private fun openShareInBrowser(vip: String, port: Int) {
        val intent = Intent(this, ShareWebActivity::class.java).apply {
            putExtra(ShareWebActivity.EXTRA_VIP, vip)
            putExtra(ShareWebActivity.EXTRA_PORT, port)
            putExtra(ShareWebActivity.EXTRA_TITLE, "共享盘 · $vip")
        }
        startActivity(intent)
    }

    private fun refreshShareDirDisplay(uri: String?) {
        binding.tvShareDir.text = if (uri.isNullOrEmpty()) {
            ShareDirManager.getDefaultShareDir(this).absolutePath
        } else {
            try {
                java.net.URLDecoder.decode(uri, "UTF-8")
            } catch (e: Exception) {
                uri
            }
        }
    }

    private fun toast(msg: String) {
        Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
    }
}
