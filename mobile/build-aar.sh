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

# ============================================================
# 1. 检查 Go
# ============================================================
if ! command -v go >/dev/null 2>&1; then
    err "未找到 go 命令。请先安装 Go 1.21+"
    echo "  https://go.dev/dl/"
    exit 1
fi
GO_VERSION=$(go version | awk '{print $3}')
log "Go 版本: $GO_VERSION"

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
# 5. gomobile init（首次或 NDK 变更后需要）
# ============================================================
# 检测是否需要 init：如果 gomobile 缓存不存在，则执行
GOMOBILE_CACHE="${GOPATH:-$HOME/go}/pkg/gomobile"
if [ ! -d "$GOMOBILE_CACHE" ]; then
    log "首次使用，执行 gomobile init（可能需要几分钟）..."
    gomobile init
    ok "gomobile 初始化完成"
else
    log "gomobile 已初始化（跳过 init）"
    log "  如需强制重新初始化，删除: $GOMOBILE_CACHE"
fi

# ============================================================
# 6. 构建 AAR
# ============================================================
mkdir -p "$OUTPUT_DIR"

# 删除旧 AAR
if [ -f "$OUTPUT_AAR" ]; then
    rm -f "$OUTPUT_AAR"
    log "已删除旧的 AAR"
fi

log "开始构建 AAR（首次约 3-5 分钟，因为要编译 gVisor）..."
START_TIME=$(date +%s)

gomobile bind \
    -target=android \
    -androidapi 26 \
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
