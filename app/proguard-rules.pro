# ============================================================
# gomobile 生成的绑定类
# 包名 = com.n2n.mobile（由 go.mod 的 module 路径决定）
# 实际类：com.n2n.mobile.Client / Config / Mobile
# ============================================================
-keep class com.n2n.mobile.** { *; }

# ============================================================
# Go runtime 侧
# ============================================================
-keep class go.** { *; }
-dontwarn go.**

# ============================================================
# 消除 gomobile 绑定类里的 warning
# ============================================================
-dontwarn com.n2n.mobile.**

# ============================================================
# Kotlin 反射相关（R8 需要保留元数据）
# ============================================================
-keepattributes *Annotation*
-keepattributes Signature
-keepattributes InnerClasses
-keepattributes EnclosingMethod
