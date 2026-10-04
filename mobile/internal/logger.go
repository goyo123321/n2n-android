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
)

// AppendLog 追加一行日志（带时间戳）
func AppendLog(msg string) {
	logBufferMu.Lock()
	defer logBufferMu.Unlock()

	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	logBuffer = append(logBuffer, line)

	if len(logBuffer) > maxLogLines {
		logBuffer = logBuffer[len(logBuffer)-maxLogLines:]
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

// ClearLogs 清空日志
func ClearLogs() {
	logBufferMu.Lock()
	defer logBufferMu.Unlock()
	logBuffer = nil
}

// logWriter 实现 io.Writer，把所有 log 包输出重定向到缓冲区
type logWriter struct{}

func (w logWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	AppendLog(msg)
	// 同时输出到 stderr（Android 上会走 logcat）
	_, _ = fmt.Fprintln(os.Stderr, msg)
	return len(p), nil
}

// initLogRedirection 在 init() 里调用，把 log 包输出重定向
func initLogRedirection() {
	log.SetOutput(io.MultiWriter(os.Stderr, logWriter{}))
	log.SetFlags(0) // 不用 log 包自带的时间戳，我们自己加
}

func init() {
	initLogRedirection()
}
