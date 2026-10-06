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

const maxLogLines = 500

var (
	logBuffer   []string
	logBufferMu sync.RWMutex
	logFilePath string
	logFileMu   sync.Mutex
)

func setLogFilePath(path string) {
	logFileMu.Lock()
	logFilePath = path
	logFileMu.Unlock()

	if path == "" {
		return
	}

	// 读回历史日志
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

// AppendLog 追加一行日志（带时间戳），同步写文件
func AppendLog(msg string) {
	logBufferMu.Lock()
	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > maxLogLines {
		logBuffer = logBuffer[len(logBuffer)-maxLogLines:]
	}
	logBufferMu.Unlock()

	logFileMu.Lock()
	path := logFilePath
	logFileMu.Unlock()

	if path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			_, _ = f.WriteString(line + "\n")
			_ = f.Close()
		}
	}
}

// GetLogs 每次从文件重读，这样 Kotlin ktLog 写的内容也能被看到
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
	return strings.Join(logBuffer, "\n")
}

// ClearLogs 清空日志（含文件）
func ClearLogs() {
	logBufferMu.Lock()
	logBuffer = nil
	logBufferMu.Unlock()

	logFileMu.Lock()
	path := logFilePath
	logFileMu.Unlock()

	if path != "" {
		_ = os.Remove(path)
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

// ★ init 尝试多个候选路径
//   优先 /data/user/0/（Android 7+ 真实路径，SELinux 上下文更可能正确）
//   fallback /data/data/（旧版本或符号链接可用时）
func init() {
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

		// 写入测试
		testFile := filepath.Join(dir, ".write_test")
		f, err := os.Create(testFile)
		if err != nil {
			continue
		}
		_ = f.Close()
		_ = os.Remove(testFile)

		// 可用，选它
		setLogFilePath(p)
		return
	}
}
