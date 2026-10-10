# ============================================================
# gomobile 生成的 Java 桥接类
# ============================================================

-keep class com.n2n.mobile.** { *; }
-keep class go.** { *; }
-dontwarn go.**
-dontwarn com.n2n.mobile.**

# ★ Protector 是 gomobile 回调接口，Kotlin 实现类必须保留，
#   R8 会把实现类的方法名混淆掉，导致 JNI 侧调用 NoSuchMethodError
-keep interface com.n2n.mobile.Protector { *; }
-keep class * implements com.n2n.mobile.Protector { *; }

# ★ Client / Config 的方法签名 JNI 依赖，必须保留
-keepclassmembers class com.n2n.mobile.Client {
    public <methods>;
}
-keepclassmembers class com.n2n.mobile.Config {
    <fields>;
    public <methods>;
}
-keepclassmembers class com.n2n.mobile.Mobile {
    public static <methods>;
}

# ============================================================
# 常规属性保留
# ============================================================

-keepattributes *Annotation*
-keepattributes Signature
-keepattributes InnerClasses
-keepattributes EnclosingMethod

# ============================================================
# 我们的 App 类（如果将来开 minify，保护反射入口）
# ============================================================

-keep class com.n2n.android.N2nVpnService { *; }
-keep class com.n2n.android.MainActivity { *; }
-keep class com.n2n.android.LogActivity { *; }
-keep class com.n2n.android.N2nController { *; }
-keep class com.n2n.android.Prefs { *; }

# AndroidX Material 的已知警告
-dontwarn androidx.**
-dontwarn com.google.android.material.**
