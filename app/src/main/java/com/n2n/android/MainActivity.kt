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
import android.widget.EditText
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.app.AppCompatDelegate
import com.n2n.mobile.databinding.ActivityMainBinding
import com.n2n.mobile.databinding.ItemPeerBinding
import org.json.JSONArray

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding

    // 节流刷新节点列表
    private val peersTicker = object : Runnable {
        override fun run() {
            if (N2nController.isRunning()) {
                refreshPeers()
            }
            binding.root.postDelayed(this, 3000)
        }
    }

    // VPN 权限
    private val vpnPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { result ->
        if (result.resultCode == Activity.RESULT_OK) {
            startVpnService()
        } else {
            toast("用户拒绝 VPN 权限")
        }
    }

    // 通知权限（Android 13+）
    private val notifPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (!granted) {
            // 用户拒绝通知权限，不影响功能，只是收不到上传进度提示
        }
    }

    // SAF 目录选择
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
        // ★ 必须在 super.onCreate 之前应用主题
        applyThemeMode(Prefs.loadThemeMode(this))
        super.onCreate(savedInstanceState)

        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // 加载配置
        loadConfig(savedInstanceState)
        handleIntentParams(intent)
        refreshShareDirDisplay(Prefs.loadShareDirUri(this))

        // 通知权限（Android 13+）
        requestNotifPermissionIfNeeded()

        // 首次启动提示
        showFirstLaunchDialogIfNeeded()

        // Toolbar 菜单（主题切换）
        binding.toolbar.inflateMenu(R.menu.menu_main)
        binding.toolbar.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                R.id.action_theme -> {
                    showThemePicker()
                    true
                }
                else -> false
            }
        }

        // 按钮
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

        // 注册上传进度监听
        N2nController.setProgressListener(UploadProgressListener(this))

        // 初始状态
        refreshStatus()

        // 启动节点列表轮询
        binding.root.postDelayed(peersTicker, 3000)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleIntentParams(intent)
    }

    override fun onResume() {
        super.onResume()
        refreshStatus()
        if (N2nController.isRunning()) {
            refreshPeers()
        }
    }

    override fun onPause() {
        super.onPause()
        saveCurrentInput()
    }

    override fun onDestroy() {
        super.onDestroy()
        binding.root.removeCallbacks(peersTicker)
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        outState.putString("signalingUrl", binding.etSignalingUrl.text.toString())
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

    // ============================================================
    // 首次启动提示
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

    // ============================================================
    // 配置加载 / 保存
    // ============================================================

    private fun loadConfig(savedInstanceState: Bundle?) {
        if (savedInstanceState != null) {
            binding.etSignalingUrl.setText(savedInstanceState.getString("signalingUrl", ""))
            binding.etRoomId.setText(savedInstanceState.getString("roomId", ""))
            binding.etClientId.setText(savedInstanceState.getString("clientId", ""))
            binding.etNodeName.setText(savedInstanceState.getString("nodeName", ""))
            binding.etConnectToken.setText(savedInstanceState.getString("connectToken", ""))
            return
        }
        val cfg = Prefs.load(this)
        binding.etSignalingUrl.setText(cfg.signalingUrl)
        binding.etRoomId.setText(cfg.roomId)
        binding.etClientId.setText(cfg.clientId)
        binding.etNodeName.setText(cfg.nodeName)
        binding.etConnectToken.setText(cfg.connectToken)
    }

    private fun handleIntentParams(intent: Intent?) {
        intent ?: return

        // n2n://connect?...
        if (intent.action == Intent.ACTION_VIEW && intent.data != null) {
            val data = intent.data!!
            if (data.scheme == "n2n" && data.host == "connect") {
                data.getQueryParameter("url")?.let { binding.etSignalingUrl.setText(it) }
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

        // adb --es 参数
        val url = intent.getStringExtra(N2nVpnService.EXTRA_SIGNALING_URL)
        val room = intent.getStringExtra(N2nVpnService.EXTRA_ROOM_ID)
        val cid = intent.getStringExtra(N2nVpnService.EXTRA_CLIENT_ID)
        val name = intent.getStringExtra(N2nVpnService.EXTRA_NODE_NAME)
        val token = intent.getStringExtra(N2nVpnService.EXTRA_CONNECT_TOKEN)

        var changed = false
        if (!url.isNullOrEmpty()) { binding.etSignalingUrl.setText(url); changed = true }
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
        Prefs.save(this, Prefs.Config(
            signalingUrl = binding.etSignalingUrl.text.toString().trim(),
            roomId = binding.etRoomId.text.toString().trim(),
            clientId = binding.etClientId.text.toString().trim(),
            nodeName = binding.etNodeName.text.toString().trim(),
            connectToken = binding.etConnectToken.text.toString().trim(),
        ))
    }

    // ============================================================
    // 启动 / 停止 VPN
    // ============================================================

    private fun saveThenRequest() {
        saveCurrentInput()
        requestVpnPermission()
    }

    private fun requestVpnPermission() {
        val url = binding.etSignalingUrl.text.toString().trim()
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

        val intent = Intent(this, N2nVpnService::class.java).apply {
            action = N2nVpnService.ACTION_START
            putExtra(N2nVpnService.EXTRA_SIGNALING_URL, binding.etSignalingUrl.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_ROOM_ID, binding.etRoomId.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_CLIENT_ID, binding.etClientId.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_NODE_NAME, binding.etNodeName.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_CONNECT_TOKEN, binding.etConnectToken.text.toString().trim())
            putExtra(N2nVpnService.EXTRA_SHARE_DIR, shareDir)
        }
        startForegroundService(intent)
        toast("正在启动...")
        binding.root.postDelayed({ refreshStatus() }, 1500)
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

            // 共享盘地址
            val vip = N2nController.getVirtualIP()
            if (vip.isNotEmpty()) {
                binding.tvShareUrl.text = "共享盘: http://$vip:9090/"
                binding.tvShareUrl.visibility = View.VISIBLE
            } else {
                binding.tvShareUrl.visibility = View.GONE
            }

            binding.btnStart.visibility = View.GONE
            binding.btnStop.visibility = View.VISIBLE

            refreshPeers()
        } else {
            binding.tvStatus.text = getString(R.string.status_stopped)
            binding.tvStatus.setTextColor(getColor(R.color.brand_text_dim))
            binding.tvVirtualIp.visibility = View.GONE
            binding.tvClientId.visibility = View.GONE
            binding.tvShareUrl.visibility = View.GONE
            binding.cardStatus.setStrokeColor(getColor(R.color.brand_border))

            binding.btnStart.visibility = View.VISIBLE
            binding.btnStop.visibility = View.GONE

            refreshPeers()   // 显示空列表
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
        val peers = try {
            JSONArray(json)
        } catch (e: Exception) {
            JSONArray()
        }

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

    private fun openShareInBrowser(vip: String, port: Int) {
        val url = "http://$vip:$port/"
        try {
            val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
            startActivity(intent)
        } catch (e: Exception) {
            toast("无法打开浏览器: ${e.message}")
        }
    }

    // ============================================================
    // 共享目录显示
    // ============================================================

    private fun refreshShareDirDisplay(uri: String?) {
        binding.tvShareDir.text = if (uri.isNullOrEmpty()) {
            ShareDirManager.getDefaultShareDir(this).absolutePath
        } else {
            uri
        }
    }

    // ============================================================
    // 工具
    // ============================================================

    private fun toast(msg: String) {
        Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
    }
}
