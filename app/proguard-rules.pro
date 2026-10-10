# ============================================================
# gomobile 生成的 Java 桥接类
# ============================================================

-keep class com.n2n.mobile.** { *; }
-keep class go.** { *; }
-dontwarn go.**
-dontwarn com.n2n.mobile.**

-keep interface com.n2n.mobile.Protector { *; }
-keep class * implements com.n2n.mobile.Protector {
    public boolean protect(long);
}

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
# Android 组件（manifest 声明的）
# ============================================================

-keep class com.n2n.android.N2nVpnService { *; }
-keep class com.n2n.android.MainActivity { *; }
-keep class com.n2n.android.LogActivity { *; }

# ============================================================
# 元数据
# ============================================================

-keepattributes *Annotation*
-keepattributes Signature
-keepattributes InnerClasses
-keepattributes EnclosingMethod

-dontwarn androidx.**
-dontwarn com.google.android.material.**
