package com.n2n.android

import android.Manifest
import android.app.Activity
import android.content.Context
import android.content.Intent
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

    private var lastPeersJson: String = ""

    private val peersTicker = object : Runnable {
        override fun run() {
            if (isFinishing || isDestroyed) return
            if (N2nController.isRunning()) refreshPeers()
            binding.root.postDelayed(this, 3000)
        }
    }

    private val statusTicker = object : Runnable {
        override fun run() {
            if (isFinishing || isDestroyed) return
            refreshStatus()
            binding.root.postDelayed(this, 1000)
        }
    }

    private val vpnPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { result ->
        if (result.resultCode == Activity.RESULT_OK) startVpnService()
        else toast(getString(R.string.dialog_vpn_denied))
    }

    private val notifPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { }

    override fun onCreate(savedInstanceState: Bundle?) {
        applyThemeMode(Prefs.loadThemeMode(this))
        super.onCreate(savedInstanceState)

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

        refreshStatus()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleIntentParams(intent)
    }

    override fun onStart() {
        super.onStart()
        binding.root.removeCallbacks(peersTicker)
        binding.root.removeCallbacks(statusTicker)
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
            notifPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
        }
    }

    @android.annotation.SuppressLint("BatteryLife")
    private fun requestIgnoreBatteryOptimizationIfNeeded() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return
        val pm = getSystemService(Context.POWER_SERVICE)
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

    private fun checkSignalingUrl() {
        val currentUrl = binding.etSignalingUrl.text.toString().trim()
        if (currentUrl.isNotEmpty()) return

        AlertDialog.Builder(this)
            .setTitle(getString(R.string.first_use_title))
            .setMessage(getString(R.string.first_use_message))
            .setPositiveButton(getString(R.string.first_use_fill)) { _, _ ->
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
            .setNegativeButton(getString(R.string.first_use_later), null)
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

        // 深链：n2n://connect?url=...&token=...&room=...&auto=1
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

        // Intent extra
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
            // ★ 单行改动：硬编码中文 → strings
            toast(getString(R.string.dialog_external_updated))
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

        // CONNECT_TOKEN 校验（空 = 未启用，跳过）
        val token = binding.etConnectToken.text.toString().trim()
        if (token.isNotEmpty() && !isValidConnectToken(token)) {
            toast(getString(R.string.token_invalid))
            binding.etConnectToken.requestFocus()
            return
        }

        val intent = VpnService.prepare(this)
        if (intent != null) {
            vpnPermissionLauncher.launch(intent)
        } else {
            startVpnService()
        }
    }

    // CONNECT_TOKEN 是任意可打印 ASCII 字符串，只做基本校验
    private fun isValidConnectToken(s: String): Boolean {
        if (s.isEmpty()) return true
        if (s.length > 256) return false
        return s.all { it.code in 33..126 }
    }

    private fun startVpnService() {
        val preferredIp = binding.etPreferredIp.text.toString().trim()
        val token = binding.etConnectToken.text.toString().trim()

        val intent = Intent(this, N2nVpnService::class.java).apply {
            action = N2nVpnService.ACTION_START
            putExtra(N2nVpnService.EXTRA_SIGNALING_URL, binding.etSignalingUrl.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_PREFERRED_IP, preferredIp)
            putExtra(N2nVpnService.EXTRA_ROOM_ID, binding.etRoomId.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_CLIENT_ID, binding.etClientId.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_NODE_NAME, binding.etNodeName.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_CONNECT_TOKEN, token)
        }
        startForegroundService(intent)
        toast(getString(R.string.dialog_starting))
        binding.root.postDelayed({ refreshStatus() }, 500)
    }

    private fun stopVpnService() {
        val intent = Intent(this, N2nVpnService::class.java).apply {
            action = N2nVpnService.ACTION_STOP
        }
        startService(intent)
        toast(getString(R.string.dialog_stopped))
        binding.root.postDelayed({ refreshStatus() }, 500)
    }

    // ============================================================
    // UI 刷新
    // ============================================================

    private fun refreshStatus() {
        if (isFinishing || isDestroyed) return
        val running = N2nController.isRunning()

        if (running) {
            binding.tvStatus.text = getString(R.string.status_running)
            binding.tvStatus.setTextColor(getColor(R.color.brand_success))
            binding.tvVirtualIp.text = "虚拟 IP: ${N2nController.getVirtualIP()}"
            binding.tvClientId.text = "Client ID: ${N2nController.getClientID()}"
            binding.tvVirtualIp.visibility = View.VISIBLE
            binding.tvClientId.visibility = View.VISIBLE
            binding.cardStatus.setStrokeColor(getColor(R.color.brand_success))

            binding.btnStart.visibility = View.GONE
            binding.btnStop.visibility = View.VISIBLE

            // 节点卡片：运行中显示
            binding.cardPeers.visibility = View.VISIBLE

            // 保存按钮：运行中禁用（改了也不会应用，需要停止后再启动）
            binding.btnSave.isEnabled = false
            binding.btnSave.alpha = 0.5f
        } else {
            binding.tvStatus.text = getString(R.string.status_stopped)
            binding.tvStatus.setTextColor(getColor(R.color.brand_text_dim))
            binding.tvVirtualIp.visibility = View.GONE
            binding.tvClientId.visibility = View.GONE

            binding.cardStatus.setStrokeColor(getColor(R.color.brand_border))

            binding.btnStart.visibility = View.VISIBLE
            binding.btnStop.visibility = View.GONE

            // 节点卡片：未运行隐藏
            binding.cardPeers.visibility = View.GONE

            // 保存按钮：未运行可用
            binding.btnSave.isEnabled = true
            binding.btnSave.alpha = 1.0f
        }
    }

    private fun refreshPeers() {
        if (isFinishing || isDestroyed) return

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

        val peers = try { JSONArray(json) } catch (e: Exception) { JSONArray() }

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
            for (i in 0 until peers.length()) {
                val p = peers.getJSONObject(i)
                val row = binding.peersContainer.getChildAt(i)

                val tvCode = row.findViewById<android.widget.TextView>(R.id.tvPeerCode)
                val tvVip = row.findViewById<android.widget.TextView>(R.id.tvPeerVip)
                val tvMeta = row.findViewById<android.widget.TextView>(R.id.tvPeerMeta)
                val tvBadge = row.findViewById<android.widget.TextView>(R.id.tvPeerBadge)

                tvCode.text = p.optString("code", "?")
                tvVip.text = p.optString("vip", "--")
                val connType = p.optString("connType", "unknown")
                val natType = p.optString("natType", "unknown")
                tvMeta.text = natType

                applyBadge(tvBadge, connType)
            }
            return
        }

        binding.peersContainer.removeAllViews()
        for (i in 0 until peers.length()) {
            val p = peers.getJSONObject(i)
            val item = ItemPeerBinding.inflate(inflater, binding.peersContainer, false)

            item.tvPeerCode.text = p.optString("code", "?")
            item.tvPeerVip.text = p.optString("vip", "--")

            val connType = p.optString("connType", "unknown")
            val natType = p.optString("natType", "unknown")
            item.tvPeerMeta.text = natType

            applyBadge(item.tvPeerBadge, connType)

            binding.peersContainer.addView(item.root)
        }
    }

    // 根据连接方式给徽章上色
    private fun applyBadge(badge: android.widget.TextView, connType: String) {
        when (connType.lowercase()) {
            "p2p" -> {
                badge.text = "P2P"
                badge.setBackgroundColor(getColor(R.color.badge_p2p_bg))
                badge.setTextColor(getColor(R.color.badge_p2p_fg))
            }
            "turn" -> {
                badge.text = "TURN"
                badge.setBackgroundColor(getColor(R.color.badge_turn_bg))
                badge.setTextColor(getColor(R.color.badge_turn_fg))
            }
            "relay" -> {
                badge.text = "WS"
                badge.setBackgroundColor(getColor(R.color.badge_ws_bg))
                badge.setTextColor(getColor(R.color.badge_ws_fg))
            }
            else -> {
                badge.text = "?"
                badge.setBackgroundColor(getColor(R.color.brand_surface_2))
                badge.setTextColor(getColor(R.color.brand_text_dim))
            }
        }
    }

    private fun toast(msg: String) {
        Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
    }
}
