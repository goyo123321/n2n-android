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

// SetLogFile 设置日志文件路径
// 如果文件已存在，读回最近 maxLogLines 行到内存
func SetLogFile(path string) {
	logFileMu.Lock()
	logFilePath = path
	logFileMu.Unlock()

	if path == "" {
		return
	}

	// 从文件读回历史
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

// AppendLog 追加一行日志（带时间戳），同时写文件
func AppendLog(msg string) {
	logBufferMu.Lock()
	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > maxLogLines {
		logBuffer = logBuffer[len(logBuffer)-maxLogLines:]
	}
	logBufferMu.Unlock()

	// ★ 同步写文件（崩溃时能保住）
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

// GetLogs 返回全部日志（换行拼接）
func GetLogs() string {
	logBufferMu.RLock()
	defer logBufferMu.RUnlock()
	if len(logBuffer) == 0 {
		return ""
	}
	return strings.Join(logBuffer, "\n")
}

// ClearLogs 清空日志（内存 + 文件）
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

// logWriter 实现 io.Writer，把所有 log 包输出重定向到缓冲区
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
}
