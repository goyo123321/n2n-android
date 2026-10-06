package com.n2n.android

import android.net.IpPrefix
import android.net.VpnService
import java.net.InetAddress

/**
 * 隔离 API 29+ 的 VpnService.Builder.excludeRoute
 *
 * 原因：ART 在类加载时验证字节码，如果 N2nVpnService 直接引用 excludeRoute，
 * 在 API 28 上会 NoSuchMethodError（不管有没有 if 判断）。
 *
 * 解法：把引用放到这个独立类里，API < 29 时不加载这个类 → 不验证 → 不崩。
 */
object VpnExcludeHelper {

    fun exclude(builder: VpnService.Builder, cidr: String) {
        val slashIdx = cidr.indexOf('/')
        if (slashIdx < 0) return
        val ipStr = cidr.substring(0, slashIdx)
        val prefixLen = cidr.substring(slashIdx + 1).toInt()
        val addr = InetAddress.getByName(ipStr)
        val prefix = IpPrefix(addr, prefixLen)
        builder.excludeRoute(prefix)
    }
}
