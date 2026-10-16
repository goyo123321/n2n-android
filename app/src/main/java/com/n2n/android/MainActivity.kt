package com.n2n.android

import android.Manifest
import android.app.Activity
import android.content.Context
import android.content.Intent
import android.net.VpnService
import android.os.Build
import android.os.Bundle
import android.util.Log
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

    companion object {
        private const val TAG = "MainActivity"
    }

    private lateinit var binding: ActivityMainBinding

    private var lastPeersJson: String = ""

    @Volatile private var statusTicking = false
    @Volatile private var peersTicking = false

    private var pendingAutoStart: Runnable? = null

    private val peersTicker = object : Runnable {
        override fun run() {
            if (!peersTicking) return
            if (isFinishing || isDestroyed || !::binding.isInitialized) return
            if (N2nController.isRunning()) refreshPeers()
            if (peersTicking) binding.root.postDelayed(this, 3000)
        }
    }

    private val statusTicker = object : Runnable {
        override fun run() {
            if (!statusTicking) return
            if (isFinishing || isDestroyed || !::binding.isInitialized) return
            refreshStatus()
            if (statusTicking) binding.root.postDelayed(this, 1000)
        }
    }

    private val vpnPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { result ->
        if (isFinishing || isDestroyed) return@registerForActivityResult
        if (result.resultCode == Activity.RESULT_OK) startVpnService()
        else toast(getString(R.string.dialog_vpn_denied))
    }

    private val notifPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { }

    override fun onCreate(savedInstanceState: Bundle?) {
        applyThemeMode(Prefs.loadThemeMode(this))
        super.onCreate(savedInstanceState)

        N2nController.applyLogFilePath(this)
        N2nController.applySystemTimezone()

        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        loadConfig(savedInstanceState)
        handleIntentParams(intent)

        requestNotifPermissionIfNeeded()
        requestIgnoreBatteryOptimizationIfNeeded()
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
            toast(getString(R.string.dialog_saved))
        }

        // SOCKS5 开关联动
        binding.switchSocks5.setOnCheckedChangeListener { _, checked ->
            if (!::binding.isInitialized) return@setOnCheckedChangeListener
            if (checked) {
                if (binding.etSocks5Listen.text.toString().trim().isEmpty()) {
                    binding.etSocks5Listen.setText(getString(R.string.socks5_default_listen))
                }
                binding.tilSocks5Listen.visibility = View.VISIBLE
            } else {
                binding.etSocks5Listen.setText("")
                binding.tilSocks5Listen.visibility = View.GONE
            }
        }
        binding.tilSocks5Listen.visibility =
            if (binding.switchSocks5.isChecked) View.VISIBLE else View.GONE

        refreshStatus()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handleIntentParams(intent)
    }

    override fun onStart() {
        super.onStart()
        if (!::binding.isInitialized) return
        binding.root.removeCallbacks(statusTicker)
        binding.root.removeCallbacks(peersTicker)
        statusTicking = true
        peersTicking = true
        binding.root.postDelayed(statusTicker, 1000)
        binding.root.postDelayed(peersTicker, 3000)
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

    override fun onStop() {
        super.onStop()
        statusTicking = false
        peersTicking = false
        cancelPendingAutoStart()
        if (!::binding.isInitialized) return
        binding.root.removeCallbacks(peersTicker)
        binding.root.removeCallbacks(statusTicker)
    }

    override fun onDestroy() {
        statusTicking = false
        peersTicking = false
        cancelPendingAutoStart()
        if (::binding.isInitialized) {
            binding.root.removeCallbacks(peersTicker)
            binding.root.removeCallbacks(statusTicker)
        }
        super.onDestroy()
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        if (!::binding.isInitialized) return
        outState.putString("signalingUrl", binding.etSignalingUrl.text.toString())
        outState.putString("preferredIp", binding.etPreferredIp.text.toString())
        outState.putString("roomId", binding.etRoomId.text.toString())
        outState.putString("clientId", binding.etClientId.text.toString())
        outState.putString("nodeName", binding.etNodeName.text.toString())
        outState.putString("connectToken", binding.etConnectToken.text.toString())
        outState.putString("socks5Listen", binding.etSocks5Listen.text.toString())
    }

    // ============================================================
    // 深链延迟启动
    // ============================================================

    private fun scheduleAutoStart(delayMs: Long = 300) {
        cancelPendingAutoStart()
        if (!::binding.isInitialized) return
        val r = Runnable {
            pendingAutoStart = null
            if (isFinishing || isDestroyed || !::binding.isInitialized) return@Runnable
            requestVpnPermission()
        }
        pendingAutoStart = r
        binding.root.postDelayed(r, delayMs)
    }

    private fun cancelPendingAutoStart() {
        pendingAutoStart?.let { r ->
            if (::binding.isInitialized) {
                try { binding.root.removeCallbacks(r) } catch (_: Throwable) {}
            }
        }
        pendingAutoStart = null
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
        if (AppCompatDelegate.getDefaultNightMode() != nightMode) {
            AppCompatDelegate.setDefaultNightMode(nightMode)
        }
    }

    private fun showThemePicker() {
        val current = Prefs.loadThemeMode(this)
        val options = arrayOf(
            getString(R.string.theme_follow_system),
            getString(R.string.theme_light),
            getString(R.string.theme_dark)
        )
        AlertDialog.Builder(this)
            .setTitle(getString(R.string.theme_title))
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
            try {
                notifPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            } catch (t: Throwable) {
                Log.w(TAG, "notifPermissionLauncher.launch failed", t)
            }
        }
    }

    // ============================================================
    // ★ Bug 3 修复：电池优化提示只弹一次
    // ============================================================
    //
    // 旧版：每次冷启动都检查 isIgnoringBatteryOptimizations，未加入白名单就弹
    //       系统对话框。用户点"不允许"后下次进 App 又弹——骚扰用户。
    //
    // 新版：加"已询问"标记，无论用户是否同意，只弹一次。
    //
    @android.annotation.SuppressLint("BatteryLife")
    private fun requestIgnoreBatteryOptimizationIfNeeded() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return

        // ★ 只问一次
        if (Prefs.hasAskedBatteryOptimization(this)) return

        try {
            val pm = getSystemService(Context.POWER_SERVICE) as android.os.PowerManager
            val pkg = packageName
            if (pm.isIgnoringBatteryOptimizations(pkg)) {
                // 已在白名单，标记一下避免后续再检查
                Prefs.markBatteryOptimizationAsked(this)
                return
            }

            // ★ 先标记，避免弹窗过程中 Activity 重建导致重入
            Prefs.markBatteryOptimizationAsked(this)

            try {
                val intent = Intent(
                    android.provider.Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS
                ).apply {
                    data = android.net.Uri.parse("package:$pkg")
                }
                startActivity(intent)
            } catch (t: Throwable) {
                try {
                    startActivity(Intent(android.provider.Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS))
                } catch (_: Throwable) {}
            }
        } catch (t: Throwable) {
            Log.w(TAG, "requestIgnoreBatteryOptimization failed", t)
        }
    }

    // ============================================================
    // ★ Bug 1 修复：首次使用对话框只弹一次
    // ============================================================
    //
    // 旧版：只检查 hasEverStartedVpn，未成功启动过 VPN 就每次都弹
    //       （setCancelable(false) 必须选一个按钮，用户体验很差）。
    //       Prefs.hasSeenFirstUseDialog 定义了但从未被调用。
    //
    // 新版：加 hasSeenFirstUseDialog 检查，无论用户点"现在就填"还是
    //       "稍后再说"，都标记已看过。
    //
    private fun checkSignalingUrl() {
        if (!::binding.isInitialized) return
        val currentUrl = binding.etSignalingUrl.text.toString().trim()
        if (currentUrl.isNotEmpty()) return
        if (Prefs.hasEverStartedVpn(this)) return

        // ★ 关键：只看一次
        if (Prefs.hasSeenFirstUseDialog(this)) return

        AlertDialog.Builder(this)
            .setTitle(getString(R.string.first_use_title))
            .setMessage(getString(R.string.first_use_message))
            .setPositiveButton(getString(R.string.first_use_fill)) { _, _ ->
                Prefs.markFirstUseDialogSeen(this)   // ★ 点"现在就填"也标记
                if (isFinishing || isDestroyed || !::binding.isInitialized) return@setPositiveButton
                binding.etSignalingUrl.requestFocus()
                binding.etSignalingUrl.setSelection(
                    binding.etSignalingUrl.text?.length ?: 0
                )
                try {
                    val imm = getSystemService(INPUT_METHOD_SERVICE) as InputMethodManager
                    imm.showSoftInput(
                        binding.etSignalingUrl,
                        InputMethodManager.SHOW_IMPLICIT
                    )
                } catch (_: Throwable) {}
            }
            .setNegativeButton(getString(R.string.first_use_later)) { _, _ ->
                Prefs.markFirstUseDialogSeen(this)   // ★ 点"稍后再说"也标记
            }
            .setCancelable(false)
            .show()
    }

    // ============================================================
    // 配置加载 / 保存
    // ============================================================

    private fun loadConfig(savedInstanceState: Bundle?) {
        if (!::binding.isInitialized) return
        try {
            if (savedInstanceState != null) {
                binding.etSignalingUrl.setText(savedInstanceState.getString("signalingUrl", ""))
                binding.etPreferredIp.setText(savedInstanceState.getString("preferredIp", ""))
                binding.etRoomId.setText(savedInstanceState.getString("roomId", ""))
                binding.etClientId.setText(savedInstanceState.getString("clientId", ""))
                binding.etNodeName.setText(savedInstanceState.getString("nodeName", ""))
                binding.etConnectToken.setText(savedInstanceState.getString("connectToken", ""))
                val socks5 = savedInstanceState.getString("socks5Listen", "") ?: ""
                binding.etSocks5Listen.setText(socks5)
                binding.switchSocks5.isChecked = socks5.isNotEmpty()
                return
            }
            val cfg = Prefs.load(this)
            binding.etSignalingUrl.setText(cfg.signalingUrl)
            binding.etPreferredIp.setText(Prefs.loadPreferredIp(this))
            binding.etRoomId.setText(cfg.roomId)
            binding.etClientId.setText(cfg.clientId)
            binding.etNodeName.setText(cfg.nodeName)
            binding.etConnectToken.setText(cfg.connectToken)
            val socks5 = Prefs.loadSocks5Listen(this)
            binding.etSocks5Listen.setText(socks5)
            binding.switchSocks5.isChecked = socks5.isNotEmpty()
        } catch (t: Throwable) {
            Log.e(TAG, "loadConfig failed", t)
        }
    }

    // ============================================================
    // ★ Bug 4 修复：深链空值保护
    // ============================================================
    //
    // 旧版：data.getQueryParameter("socks5")?.let { ... } 会把空字符串
    //       当成有效值，覆盖用户已配置的 SOCKS5（清空 + 关开关）。
    // 新版：用 isNullOrBlank() 判断，空值直接跳过。
    //
    private fun handleIntentParams(intent: Intent?) {
        if (!::binding.isInitialized) return
        intent ?: return

        try {
            if (intent.action == Intent.ACTION_VIEW && intent.data != null) {
                val data = intent.data!!
                if (data.scheme == "n2n" && data.host == "connect") {
                    // ★ 空值保护：只覆盖非空值
                    data.getQueryParameter("url")?.let {
                        if (it.isNotBlank()) binding.etSignalingUrl.setText(it)
                    }
                    data.getQueryParameter("ip")?.let {
                        if (it.isNotBlank()) binding.etPreferredIp.setText(it)
                    }
                    data.getQueryParameter("room")?.let {
                        if (it.isNotBlank()) binding.etRoomId.setText(it)
                    }
                    data.getQueryParameter("cid")?.let {
                        if (it.isNotBlank()) binding.etClientId.setText(it)
                    }
                    data.getQueryParameter("name")?.let {
                        if (it.isNotBlank()) binding.etNodeName.setText(it)
                    }
                    data.getQueryParameter("token")?.let {
                        if (it.isNotBlank()) binding.etConnectToken.setText(it)
                    }
                    data.getQueryParameter("socks5")?.let {
                        if (it.isNotBlank()) {
                            binding.etSocks5Listen.setText(it)
                            binding.switchSocks5.isChecked = true
                        }
                    }

                    saveCurrentInput()
                    if (data.getQueryParameter("auto") == "1") {
                        scheduleAutoStart(300)
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
            val socks5 = intent.getStringExtra(N2nVpnService.EXTRA_SOCKS5_LISTEN)

            var changed = false
            if (!url.isNullOrEmpty()) { binding.etSignalingUrl.setText(url); changed = true }
            if (!ip.isNullOrEmpty()) { binding.etPreferredIp.setText(ip); changed = true }
            if (!room.isNullOrEmpty()) { binding.etRoomId.setText(room); changed = true }
            if (!cid.isNullOrEmpty()) { binding.etClientId.setText(cid); changed = true }
            if (!name.isNullOrEmpty()) { binding.etNodeName.setText(name); changed = true }
            if (!token.isNullOrEmpty()) { binding.etConnectToken.setText(token); changed = true }
            if (!socks5.isNullOrEmpty()) {
                binding.etSocks5Listen.setText(socks5)
                binding.switchSocks5.isChecked = true
                changed = true
            }

            if (changed) {
                saveCurrentInput()
                toast(getString(R.string.dialog_external_updated))
            }
            if (intent.getBooleanExtra("auto_start", false)) {
                scheduleAutoStart(300)
            }
        } catch (t: Throwable) {
            Log.e(TAG, "handleIntentParams failed", t)
        }
    }

    private fun saveCurrentInput() {
        if (!::binding.isInitialized) return
        try {
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
            if (binding.switchSocks5.isChecked) {
                Prefs.saveSocks5Listen(this, binding.etSocks5Listen.text.toString().trim())
            } else {
                Prefs.saveSocks5Listen(this, "")
            }
        } catch (t: Throwable) {
            Log.e(TAG, "saveCurrentInput failed", t)
        }
    }

    // ============================================================
    // 启动 / 停止
    // ============================================================

    private fun saveThenRequest() {
        saveCurrentInput()
        requestVpnPermission()
    }

    private fun requestVpnPermission() {
        if (isFinishing || isDestroyed) return
        if (!::binding.isInitialized) return
        val url = binding.etSignalingUrl.text.toString().trim()

        if (url.isEmpty()) {
            toast(getString(R.string.wss_required))
            checkSignalingUrl()
            return
        }
        if (!url.startsWith("wss://") && !url.startsWith("ws://")) {
            toast(getString(R.string.wss_invalid))
            return
        }

        val room = binding.etRoomId.text.toString().trim()
        if (room.isEmpty()) {
            toast(getString(R.string.room_required))
            return
        }
        if (!room.matches(Regex("^[A-Za-z0-9_-]+$"))) {
            toast(getString(R.string.room_invalid))
            return
        }

        val token = binding.etConnectToken.text.toString().trim()
        if (token.isNotEmpty() && !isValidConnectToken(token)) {
            toast(getString(R.string.token_invalid))
            binding.etConnectToken.requestFocus()
            return
        }

        if (binding.switchSocks5.isChecked) {
            val socks5 = binding.etSocks5Listen.text.toString().trim()
            if (socks5.isEmpty() || !isValidHostPort(socks5)) {
                toast(getString(R.string.socks5_invalid))
                binding.etSocks5Listen.requestFocus()
                return
            }
        }

        try {
            val intent = VpnService.prepare(this)
            if (intent != null) {
                try {
                    vpnPermissionLauncher.launch(intent)
                } catch (t: Throwable) {
                    Log.e(TAG, "vpnPermissionLauncher.launch failed", t)
                    toast("无法请求 VPN 权限，请回到 App 重试")
                }
            } else {
                startVpnService()
            }
        } catch (t: Throwable) {
            Log.e(TAG, "VpnService.prepare failed", t)
            toast("VPN 权限请求失败: ${t.message}")
        }
    }

    private fun isValidConnectToken(s: String): Boolean {
        if (s.isEmpty()) return true
        if (s.length > 256) return false
        return s.all { it.code in 33..126 }
    }

    private fun isValidHostPort(s: String): Boolean {
        val idx = s.lastIndexOf(':')
        if (idx <= 0 || idx == s.length - 1) return false
        val port = s.substring(idx + 1).toIntOrNull() ?: return false
        if (port < 1 || port > 65535) return false
        val host = s.substring(0, idx)
        if (host.isEmpty()) return false
        return true
    }

    private fun startVpnService() {
        if (isFinishing || isDestroyed) return
        if (!::binding.isInitialized) return
        try {
            val preferredIp = binding.etPreferredIp.text.toString().trim()
            val token = binding.etConnectToken.text.toString().trim()
            val socks5 = if (binding.switchSocks5.isChecked) {
                binding.etSocks5Listen.text.toString().trim()
            } else ""

            val intent = Intent(this, N2nVpnService::class.java).apply {
                action = N2nVpnService.ACTION_START
                putExtra(N2nVpnService.EXTRA_SIGNALING_URL, binding.etSignalingUrl.text.toString().trim())
                putExtra(N2nVpnService.EXTRA_PREFERRED_IP, preferredIp)
                putExtra(N2nVpnService.EXTRA_ROOM_ID, binding.etRoomId.text.toString().trim())
                putExtra(N2nVpnService.EXTRA_CLIENT_ID, binding.etClientId.text.toString().trim())
                putExtra(N2nVpnService.EXTRA_NODE_NAME, binding.etNodeName.text.toString().trim())
                putExtra(N2nVpnService.EXTRA_CONNECT_TOKEN, token)
                putExtra(N2nVpnService.EXTRA_SOCKS5_LISTEN, socks5)
            }
            startForegroundService(intent)
            toast(getString(R.string.dialog_starting))
            binding.root.postDelayed({ refreshStatus() }, 500)
        } catch (t: Throwable) {
            Log.e(TAG, "startVpnService failed", t)
            toast("启动失败: ${t.message}")
        }
    }

    private fun stopVpnService() {
        try {
            val intent = Intent(this, N2nVpnService::class.java).apply {
                action = N2nVpnService.ACTION_STOP
            }
            startService(intent)
            toast(getString(R.string.dialog_stopped))
            if (::binding.isInitialized) {
                binding.root.postDelayed({ refreshStatus() }, 500)
            }
        } catch (t: Throwable) {
            Log.e(TAG, "stopVpnService failed", t)
        }
    }

    // ============================================================
    // UI 刷新
    // ============================================================

    // ============================================================
    // ★ Bug 2 修复：运行中隐藏整个 bottomBarIdle
    // ============================================================
    //
    // 旧版：只隐藏 btnStart，btnSave 靠 FrameLayout 恰好被 btnStop 覆盖。
    //       如果未来布局调整（margin / padding），btnSave 可能漏出来。
    // 新版：直接隐藏整组，btns 一起消失。
    //
    private fun refreshStatus() {
        if (isFinishing || isDestroyed || !::binding.isInitialized) return
        try {
            val running = N2nController.isRunning()

            if (running) {
                binding.tvStatus.text = getString(R.string.status_running)
                binding.tvStatus.setTextColor(getColor(R.color.brand_success))
                binding.cardStatus.setStrokeColor(getColor(R.color.brand_success))

                val vip = N2nController.getVirtualIP()
                if (vip.isEmpty()) {
                    binding.tvVirtualIp.text = "虚拟 IP: 获取中..."
                    binding.tvVirtualIp.setTextColor(getColor(R.color.brand_text_dim))
                } else {
                    binding.tvVirtualIp.text = "虚拟 IP: $vip"
                    binding.tvVirtualIp.setTextColor(getColor(R.color.brand_text))
                }
                binding.tvVirtualIp.visibility = View.VISIBLE

                val vn = N2nController.getVirtualNetwork()
                if (vn.isNotEmpty()) {
                    binding.tvVirtualNetwork.text = "网段: $vn"
                    binding.tvVirtualNetwork.visibility = View.VISIBLE
                } else {
                    binding.tvVirtualNetwork.visibility = View.GONE
                }

                val cid = N2nController.getClientID()
                if (cid.isNotEmpty()) {
                    binding.tvClientId.text = "Client ID: $cid"
                    binding.tvClientId.visibility = View.VISIBLE
                } else {
                    binding.tvClientId.visibility = View.GONE
                }

                // ★ 隐藏整组空闲按钮，而不是只隐藏 btnStart
                binding.bottomBarIdle.visibility = View.GONE
                binding.btnStop.visibility = View.VISIBLE
                binding.cardPeers.visibility = View.VISIBLE

                // 运行中禁止改 SOCKS5
                binding.switchSocks5.isEnabled = false
                binding.switchSocks5.alpha = 0.5f
                binding.etSocks5Listen.isEnabled = false
            } else {
                binding.tvStatus.text = getString(R.string.status_stopped)
                binding.tvStatus.setTextColor(getColor(R.color.brand_text_dim))
                binding.tvVirtualIp.visibility = View.GONE
                binding.tvVirtualNetwork.visibility = View.GONE
                binding.tvClientId.visibility = View.GONE
                binding.cardStatus.setStrokeColor(getColor(R.color.brand_border))

                // ★ 恢复整组
                binding.bottomBarIdle.visibility = View.VISIBLE
                binding.btnStop.visibility = View.GONE
                binding.cardPeers.visibility = View.GONE

                // 未运行时可改 SOCKS5
                binding.switchSocks5.isEnabled = true
                binding.switchSocks5.alpha = 1.0f
                binding.etSocks5Listen.isEnabled = true
            }
        } catch (t: Throwable) {
            Log.e(TAG, "refreshStatus failed", t)
        }
    }

    private fun refreshPeers() {
        if (isFinishing || isDestroyed || !::binding.isInitialized) return
        try {
            if (!N2nController.isRunning()) {
                lastPeersJson = ""
                binding.tvPeerCount.text = ""
                binding.tvPeersEmpty.visibility = View.VISIBLE
                binding.peersContainer.removeAllViews()
                return
            }

            val json = N2nController.getPeersJSON()
            if (json == lastPeersJson) return
            lastPeersJson = json

            val peers = try { JSONArray(json) } catch (t: Throwable) { JSONArray() }

            binding.tvPeerCount.text = getString(R.string.peer_count_format, peers.length())

            if (peers.length() == 0) {
                binding.tvPeersEmpty.visibility = View.VISIBLE
                binding.peersContainer.removeAllViews()
                return
            }

            binding.tvPeersEmpty.visibility = View.GONE
            val inflater = LayoutInflater.from(this)
            val existing = binding.peersContainer.childCount

            if (existing == peers.length()) {
                var needRebuild = false
                for (i in 0 until peers.length()) {
                    val p = peers.optJSONObject(i) ?: continue
                    val row = binding.peersContainer.getChildAt(i) ?: continue

                    val tvCode = row.findViewById<android.widget.TextView>(R.id.tvPeerCode)
                    val tvVip = row.findViewById<android.widget.TextView>(R.id.tvPeerVip)
                    val tvMeta = row.findViewById<android.widget.TextView>(R.id.tvPeerMeta)
                    val tvBadge = row.findViewById<android.widget.TextView>(R.id.tvPeerBadge)

                    if (tvCode == null || tvVip == null || tvMeta == null || tvBadge == null) {
                        needRebuild = true
                        break
                    }

                    tvCode.text = p.optString("code", "?")
                    tvVip.text = p.optString("vip", "--")
                    tvMeta.text = p.optString("natType", "unknown")
                    applyBadge(tvBadge, p.optString("connType", "unknown"))
                }
                if (!needRebuild) return
            }

            binding.peersContainer.removeAllViews()
            for (i in 0 until peers.length()) {
                val p = peers.optJSONObject(i) ?: continue
                val item = ItemPeerBinding.inflate(inflater, binding.peersContainer, false)
                item.tvPeerCode.text = p.optString("code", "?")
                item.tvPeerVip.text = p.optString("vip", "--")
                item.tvPeerMeta.text = p.optString("natType", "unknown")
                applyBadge(item.tvPeerBadge, p.optString("connType", "unknown"))
                binding.peersContainer.addView(item.root)
            }
        } catch (t: Throwable) {
            Log.e(TAG, "refreshPeers failed", t)
        }
    }

    private fun applyBadge(badge: android.widget.TextView?, connType: String) {
        if (badge == null) return
        try {
            val (bgRes, fgRes, label) = when (connType.lowercase()) {
                "p2p" -> Triple(R.color.badge_p2p_bg, R.color.badge_p2p_fg, "P2P")
                "p2p-avail" -> Triple(R.color.badge_ws_bg, R.color.badge_ws_fg, "P2P~")
                "turn" -> Triple(R.color.badge_turn_bg, R.color.badge_turn_fg, "TURN")
                "relay" -> Triple(R.color.badge_ws_bg, R.color.badge_ws_fg, "WS")
                else -> Triple(R.color.brand_surface_2, R.color.brand_text_dim, "?")
            }
            badge.text = label
            badge.setTextColor(getColor(fgRes))

            val bg = android.graphics.drawable.GradientDrawable().apply {
                shape = android.graphics.drawable.GradientDrawable.RECTANGLE
                setColor(getColor(bgRes))
                cornerRadius = 4f * resources.displayMetrics.density
            }
            badge.background = bg
        } catch (t: Throwable) {
            Log.e(TAG, "applyBadge failed", t)
        }
    }

    private fun toast(msg: String) {
        if (isFinishing || isDestroyed) return
        try {
            Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
        } catch (_: Throwable) {}
    }
}
