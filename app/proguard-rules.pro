# ============================================================
# gomobile 生成的 Java 桥接层
#
# gomobile bind 生成的类通过 JNI 调用 Go 侧函数，JNI 是按
# 【类名 + 方法名 + 签名】精确查找的。任何一处被 R8 重命名，
# 都会在运行时抛 UnsatisfiedLinkError / NoSuchMethodError。
# ============================================================

-keep class com.n2n.mobile.** { *; }
-keep class go.** { *; }

# 生成的类引用了一些不在 Android 标准库的类（如 Java 反射相关），
# 关掉 R8 对它们的警告，避免编译期报错
-dontwarn go.**
-dontwarn com.n2n.mobile.**

# ============================================================
# Protector 接口
#
# Kotlin 侧实现 com.n2n.mobile.Protector 并回传给 Go。
# Go 侧通过 JNI 调用该实现类的 protect(long) 方法。
# 只保留接口不够 —— 实现类的方法名也必须保留，
# 否则 JNI 按名字找不到 protect 方法。
# ============================================================

-keep interface com.n2n.mobile.Protector { *; }
-keep class * implements com.n2n.mobile.Protector {
    public boolean protect(long);
}

# ============================================================
# Android 组件
#
# 系统按 AndroidManifest.xml 里的 android:name 反射实例化。
# R8 默认会保留 manifest 中声明的组件，但显式声明更稳妥，
# 也能防止某些 AGP 版本在 aapt 阶段误判。
# ============================================================

-keep class com.n2n.android.N2nVpnService { *; }
-keep class com.n2n.android.MainActivity { *; }
-keep class com.n2n.android.LogActivity { *; }

# ============================================================
# ViewBinding / 生成类
#
# activity_main.xml → ActivityMainBinding
# activity_log.xml   → ActivityLogBinding
# item_peer.xml      → ItemPeerBinding
#
# ViewBinding 生成的类在编译期由 AGP 生成，Kotlin 直接引用。
# 一般不需要显式保留，但如果将来加了 data binding 或反射式
# inflate，可以解开下面的规则。
# ============================================================

# -keep class com.n2n.android.databinding.** { *; }

# ============================================================
# 元数据属性
#
# 保留泛型签名、注解、内部类信息，用于 Kotlin 反射和
# gomobile 生成的类的类型信息
# ============================================================

-keepattributes *Annotation*
-keepattributes Signature
-keepattributes InnerClasses
-keepattributes EnclosingMethod

# ============================================================
# 已知无害警告抑制
# ============================================================

-dontwarn androidx.**
-dontwarn com.google.android.material.**
