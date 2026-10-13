package internal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const maxLogLines = 1000
const rewriteEvery = 1000

var (
	logBuffer   []string
	logBufferMu sync.RWMutex

	logFilePath string
	logFileMu   sync.Mutex

	appendCounter int
	counterMu     sync.Mutex

	// 时区：由 Kotlin 侧通过 SetTimezoneOffset 设置
	tzMu            sync.Mutex
	tzOffsetSeconds int
	tzSet           bool
)

// SetTimezoneOffset 由 Kotlin 侧设置设备时区偏移量（秒）。
//
// gomobile 环境下 time.Local 默认为 UTC，且没有可靠途径读取 Android
// 系统时区。Kotlin 侧读取 TimeZone.getDefault() 后传入。
func SetTimezoneOffset(seconds int) {
	tzMu.Lock()
	defer tzMu.Unlock()
	if tzSet && tzOffsetSeconds == seconds {
		return
	}
	tzOffsetSeconds = seconds
	tzSet = true
	time.Local = time.FixedZone("Local", seconds)
	fmt.Fprintf(os.Stderr, "[logger] 设置时区偏移: %+d 秒\n", seconds)
}

// SetLogFilePath 由 Kotlin 侧传入 filesDir 下的日志文件绝对路径。
//
// 之前 init() 里硬编码 `/data/user/0/com.n2n.android/files/n2n.log`——
// 换包名就废了，而且 init 里做文件系统探测时机不可控。
//
// Kotlin 侧在 App 进程启动早期（N2nVpnService.onCreate / MainActivity.onCreate）
// 调用一次即可。重复调用会切换路径（同时保留已有 buffer）。
func SetLogFilePath(path string) {
	if path == "" {
		return
	}

	logFileMu.Lock()
	logFilePath = path
	logFileMu.Unlock()

	// 加载已有日志到内存 buffer（App 重启时保留历史）
	if data, err := os.ReadFile(path); err == nil {
		content := strings.TrimRight(string(data), "\n")
		if content == "" {
			return
		}
		lines := strings.Split(content, "\n")
		if len(lines) > maxLogLines {
			lines = lines[len(lines)-maxLogLines:]
		}
		logBufferMu.Lock()
		logBuffer = lines
		logBufferMu.Unlock()
	}

	fmt.Fprintf(os.Stderr, "[logger] 日志文件: %s\n", path)
}

// AppendLog 追加一行日志（带时间戳）。
func AppendLog(msg string) {
	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)

	logBufferMu.Lock()
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > maxLogLines {
		logBuffer = logBuffer[len(logBuffer)-maxLogLines:]
	}
	logBufferMu.Unlock()

	logFileMu.Lock()
	path := logFilePath
	logFileMu.Unlock()

	if path == "" {
		return
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		_, _ = f.WriteString(line + "\n")
		_ = f.Close()
	}

	counterMu.Lock()
	appendCounter++
	needRewrite := appendCounter >= rewriteEvery
	if needRewrite {
		appendCounter = 0
	}
	counterMu.Unlock()

	if needRewrite {
		rewriteLogFile(path)
	}
}

func rewriteLogFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	content := strings.TrimRight(string(data), "\n")
	if content == "" {
		return
	}
	lines := strings.Split(content, "\n")
	if len(lines) > maxLogLines {
		lines = lines[len(lines)-maxLogLines:]
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return
	}
	_ = os.Rename(tmpPath, path)
}

func GetLogs() string {
	logFileMu.Lock()
	path := logFilePath
	logFileMu.Unlock()

	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			content := strings.TrimRight(string(data), "\n")
			if content != "" {
				lines := strings.Split(content, "\n")
				if len(lines) > maxLogLines {
					lines = lines[len(lines)-maxLogLines:]
				}
				logBufferMu.Lock()
				logBuffer = lines
				logBufferMu.Unlock()
			}
		}
	}

	logBufferMu.RLock()
	defer logBufferMu.RUnlock()
	if len(logBuffer) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.Grow(len(logBuffer) * 60)
	for i, line := range logBuffer {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(line)
	}
	return sb.String()
}

func ClearLogs() {
	logBufferMu.Lock()
	logBuffer = nil
	logBufferMu.Unlock()

	logFileMu.Lock()
	path := logFilePath
	logFileMu.Unlock()

	if path != "" {
		_ = os.Remove(path)
		_ = os.Remove(path + ".tmp")
	}
}

type logWriter struct{}

func (w logWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	AppendLog(msg)
	_, _ = fmt.Fprintln(os.Stderr, msg)
	return len(p), nil
}

// init 只做日志重定向，不再做文件系统探测。
// 文件路径由 Kotlin 侧通过 SetLogFilePath 传入。
func init() {
	{
		local := time.Now()
		_, offset := local.Zone()
		fmt.Fprintf(os.Stderr,
			"[logger] 初始 TZ=%q local=%v offset=%+d秒\n",
			os.Getenv("TZ"), time.Local, offset)
	}

	log.SetOutput(io.MultiWriter(os.Stderr, logWriter{}))
	log.SetFlags(0)
}

// 保留 filepath 引用（防止未使用 import 报错）
var _ = filepath.Join
