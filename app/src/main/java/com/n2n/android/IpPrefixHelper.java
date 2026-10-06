package com.n2n.android;

import android.net.IpPrefix;

/**
 * 绕过 Kotlin 编译器解析 IpPrefix.parse 的兼容问题
 *
 * Kotlin 直接写 IpPrefix.parse(cidr) 在部分 AGP/Kotlin 组合下报
 * Unresolved reference: parse。Java 编译器无此问题。
 */
public final class IpPrefixHelper {
    private IpPrefixHelper() {}

    public static IpPrefix parse(String cidr) {
        return IpPrefix.parse(cidr);
    }
}
