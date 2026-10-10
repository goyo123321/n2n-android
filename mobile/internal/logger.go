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

	// ★ 时区：由 Kotlin 侧通过 SetTimezoneOffset 设置
	tzMu            sync.Mutex
	tzOffsetSeconds int
	tzSet           bool
)

// SetTimezoneOffset 由 Kotlin 侧设置设备时区偏移量（秒）。
//
// gomobile 环境下 time.Local 默认为 UTC，且没有可靠途径读取 Android
// 系统时区。Kotlin 侧读取 TimeZone.getDefault() 后传入。
//
// 允许重复调用（设备时区切换时会重新调用）。
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

func setLogFilePath(path string) {
	logFileMu.Lock()
	logFilePath = path
	logFileMu.Unlock()

	if path == "" {
		return
	}

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
}

// AppendLog 追加一行日志（带时间戳）。
//
// 时区：time.Local 已在 SetTimezoneOffset 里改为设备本地时区，
// 因此直接 time.Now().Format 即为本地时间。
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

func initLogRedirection() {
	log.SetOutput(io.MultiWriter(os.Stderr, logWriter{}))
	log.SetFlags(0)
}

func init() {
	{
		local := time.Now()
		_, offset := local.Zone()
		fmt.Fprintf(os.Stderr,
			"[logger] 初始 TZ=%q local=%v offset=%+d秒\n",
			os.Getenv("TZ"), time.Local, offset)
	}

	initLogRedirection()

	candidates := []string{
		"/data/user/0/com.n2n.android/files/n2n.log",
		"/data/data/com.n2n.android/files/n2n.log",
	}

	for _, p := range candidates {
		dir := filepath.Dir(p)
		_ = os.MkdirAll(dir, 0700)

		if _, err := os.Stat(dir); err != nil {
			continue
		}

		testFile := filepath.Join(dir, ".write_test")
		f, err := os.Create(testFile)
		if err != nil {
			continue
		}
		_ = f.Close()
		_ = os.Remove(testFile)

		setLogFilePath(p)
		return
	}
}
