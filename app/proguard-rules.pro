-keep class com.n2n.mobile.** { *; }
-keep class go.** { *; }
-dontwarn go.**
-dontwarn com.n2n.mobile.**

-keepattributes *Annotation*
-keepattributes Signature
-keepattributes InnerClasses
-keepattributes EnclosingMethod
