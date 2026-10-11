plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.n2n.android"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.n2n.android"
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = "1.0.0"

        ndk {
            abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64")
        }
    }

    buildTypes {
        debug {
            isMinifyEnabled = false
        }
        release {
            // ★ 当前关闭混淆：gomobile AAR 里的 Go 桥接类反射调用较多，
            //   虽然 proguard-rules.pro 已覆盖 com.n2n.mobile.**，但为了
            //   避免首次上线的兼容风险，保守起见先不开。
            //   若要开启，改成 isMinifyEnabled = true 并跑一遍真机验证。
            isMinifyEnabled = false

            // 混淆关闭时 proguardFiles 不生效，先注释掉避免误导。
            // 开启混淆后取消注释：
            // proguardFiles(
            //     getDefaultProguardFile("proguard-android-optimize.txt"),
            //     "proguard-rules.pro"
            // )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
    buildFeatures {
        viewBinding = true
    }

    // ★ Android 15+ 对 16KB page 有要求；显式关闭 legacy packaging
    packaging {
        jniLibs {
            useLegacyPackaging = false
        }
    }
}

dependencies {
    implementation(files("libs/n2nclient.aar"))
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.appcompat:appcompat:1.7.0")
    implementation("com.google.android.material:material:1.12.0")
    implementation("androidx.recyclerview:recyclerview:1.3.2")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.4")
}
