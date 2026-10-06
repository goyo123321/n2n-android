package internal

import (
	"fmt"
	"io"
	"log"
	"os"
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

// ★ GetLogs 每次从文件重读，这样 Kotlin ktLog 写的内容也能被看到
func GetLogs() string {
	// 1. 从文件重新读一次（同步 Kotlin 写入的新内容）
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

	// 2. 返回
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

func init() {
	initLogRedirection()

	const androidLogPath = "/data/data/com.n2n.android/files/n2n.log"
	if _, err := os.Stat("/data/data/com.n2n.android/files"); err == nil {
		setLogFilePath(androidLogPath)
	}
}
