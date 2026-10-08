#!/usr/bin/env bash
#
# 用 gomobile 把 mobile 包编译成 Android AAR
# 输出: app/libs/n2nclient.aar
#
# 依赖:
#   - Go 1.21+
#   - Android NDK 26.1.10909125（或兼容版本）
#   - gomobile / gobind（会自动安装）
#
# 用法:
#   export ANDROID_NDK_HOME=$HOME/Android/Sdk/ndk/26.1.10909125
#   ./build-aar.sh
#

set -euo pipefail

# ============================================================
# 颜色
# ============================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[*]${NC} $1"; }
ok()   { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1" >&2; }

# ============================================================
# 路径
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_ROOT/app/libs"
OUTPUT_AAR="$OUTPUT_DIR/n2nclient.aar"

# Java 包名（Kotlin 侧 import com.n2n.mobile.*）
JAVA_PKG="com.n2n"

# ============================================================
# 1. 检查 Go + 版本校验
# ============================================================
if ! command -v go >/dev/null 2>&1; then
    err "未找到 go 命令。请先安装 Go 1.21+"
    echo "  https://go.dev/dl/"
    exit 1
fi

GO_VERSION_RAW=$(go version | awk '{print $3}')     # 形如 go1.22.5
GO_VERSION="${GO_VERSION_RAW#go}"                    # 去掉前缀 → 1.22.5
log "Go 版本: $GO_VERSION_RAW"

# 只取 major.minor 比较
GO_MAJOR_MINOR=$(echo "$GO_VERSION" | cut -d. -f1,2)
REQUIRED="1.21"

# 用 sort -V 判断版本大小
if [ "$(printf '%s\n' "$REQUIRED" "$GO_MAJOR_MINOR" | sort -V | head -n1)" != "$REQUIRED" ]; then
    err "Go 版本过低: $GO_VERSION_RAW（需要 go$REQUIRED+）"
    exit 1
fi
ok "Go 版本满足要求 (>= $REQUIRED)"

# ============================================================
# 2. 检查 / 安装 gomobile
# ============================================================
GOBIN="$(go env GOPATH)/bin"
export PATH="$PATH:$GOBIN"

if ! command -v gomobile >/dev/null 2>&1; then
    warn "未找到 gomobile，正在安装..."
    go install golang.org/x/mobile/cmd/gomobile@latest
    go install golang.org/x/mobile/cmd/gobind@latest
    ok "gomobile 安装完成"
else
    log "gomobile: $(command -v gomobile)"
fi

# ============================================================
# 3. 检查 Android NDK
# ============================================================
NDK_PATH="${ANDROID_NDK_HOME:-${NDK_HOME:-}}"

if [ -z "$NDK_PATH" ]; then
    err "未设置 ANDROID_NDK_HOME 或 NDK_HOME"
    echo ""
    echo "示例（macOS/Linux）:"
    echo "  export ANDROID_NDK_HOME=\$HOME/Android/Sdk/ndk/26.1.10909125"
    echo ""
    echo "示例（Windows Git Bash）:"
    echo "  export ANDROID_NDK_HOME=/c/Users/<you>/AppData/Local/Android/Sdk/ndk/26.1.10909125"
    echo ""
    echo "如果没有 NDK，可以通过 Android Studio 安装："
    echo "  SDK Manager → SDK Tools → NDK (Side by side) → 勾选 26.1.10909125"
    exit 1
fi

if [ ! -d "$NDK_PATH" ]; then
    err "NDK 路径不存在: $NDK_PATH"
    exit 1
fi

if [ ! -f "$NDK_PATH/ndk-build" ] && [ ! -f "$NDK_PATH/ndk-build.cmd" ]; then
    warn "NDK 路径中找不到 ndk-build，可能不是有效的 NDK: $NDK_PATH"
    echo "  继续尝试（gomobile 可能仍能工作）..."
fi

export ANDROID_NDK_HOME="$NDK_PATH"
ok "NDK: $NDK_PATH"

# ============================================================
# 4. go mod tidy
# ============================================================
cd "$SCRIPT_DIR"

log "go mod tidy..."
go mod tidy
ok "依赖整理完成"

# ============================================================
# 5. gomobile init
# ============================================================
# 无条件执行：已初始化时 gomobile 会秒退，不浪费时间。
# 有条件跳过反而容易踩到"缓存目录存在但实际损坏"的情况。
log "执行 gomobile init（已初始化时秒退）..."
if ! gomobile init; then
    err "gomobile init 失败"
    echo ""
    echo "常见原因："
    echo "  1. NDK 路径不对：$ANDROID_NDK_HOME"
    echo "  2. Go 版本过低：需要 go1.21+"
    echo "  3. 网络问题：下载 gomobile 依赖失败"
    exit 1
fi
ok "gomobile init 完成"

# ============================================================
# 6. 构建 AAR
# ============================================================
mkdir -p "$OUTPUT_DIR"

# 删除旧 AAR
if [ -f "$OUTPUT_AAR" ]; then
    rm -f "$OUTPUT_AAR"
    log "已删除旧的 AAR"
fi

log "开始构建 AAR（首次约 3-5 分钟）..."
log "  Java 包名: ${JAVA_PKG}.mobile"
START_TIME=$(date +%s)

# ★ -javapkg com.n2n：生成的 Java/Kotlin 类包名为 com.n2n.mobile
#   不加这个参数时 gomobile 会用默认包名（go.<module> 或 mobile），
#   Kotlin 里的 `import com.n2n.mobile.Client` 会找不到类。
gomobile bind \
    -target=android \
    -androidapi 26 \
    -javapkg "$JAVA_PKG" \
    -o "$OUTPUT_AAR" \
    .

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# ============================================================
# 7. 检查结果
# ============================================================
if [ ! -f "$OUTPUT_AAR" ]; then
    err "构建失败：未生成 $OUTPUT_AAR"
    exit 1
fi

AAR_SIZE=$(du -h "$OUTPUT_AAR" | cut -f1)

# 验证包名（可选，失败不阻断）
if command -v unzip >/dev/null 2>&1; then
    TMP_JAR="$(mktemp -t classes-XXXXXX.jar)"
    if unzip -p "$OUTPUT_AAR" classes.jar > "$TMP_JAR" 2>/dev/null; then
        if unzip -l "$TMP_JAR" 2>/dev/null | grep -q "com/n2n/mobile/Client.class"; then
            ok "AAR 包名验证通过: com.n2n.mobile.Client"
        else
            warn "AAR 里未找到 com/n2n/mobile/Client.class"
            echo "  实际内容前 10 行："
            unzip -l "$TMP_JAR" 2>/dev/null | head -20 || true
        fi
    fi
    rm -f "$TMP_JAR"
fi

echo ""
ok "构建完成！"
echo ""
echo "  AAR:  $OUTPUT_AAR"
echo "  大小: $AAR_SIZE"
echo "  耗时: ${ELAPSED}s"
echo ""
echo "下一步:"
echo "  cd $PROJECT_ROOT"
echo "  ./gradlew assembleDebug"
echo ""
