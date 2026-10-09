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

// ============ 配置 ============

const logRetentionDuration = 3 * time.Minute
const maxLogLines = 3000
const logRewriteEvery = 500

// ============ 内部状态 ============

type logEntry struct {
	ts   time.Time
	line string
}

var (
	logBuffer   []logEntry
	logBufferMu sync.RWMutex

	logFilePath string
	logFileMu   sync.Mutex

	appendCounter int
	counterMu     sync.Mutex
)

// ============ 时间戳解析 ============

func parseLogTimestamp(line string) time.Time {
	if len(line) < 10 || line[0] != '[' || line[9] != ']' {
		return time.Time{}
	}
	now := time.Now()
	t, err := time.ParseInLocation("15:04:05", line[1:9], now.Location())
	if err != nil {
		return time.Time{}
	}
	return time.Date(now.Year(), now.Month(), now.Day(),
		t.Hour(), t.Minute(), t.Second(), 0, now.Location())
}

// ============ 文件路径设置 ============

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
		cutoff := time.Now().Add(-logRetentionDuration)
		entries := make([]logEntry, 0, len(lines))
		for _, line := range lines {
			ts := parseLogTimestamp(line)
			if ts.IsZero() || ts.Before(cutoff) {
				continue
			}
			entries = append(entries, logEntry{ts: ts, line: line})
		}
		if len(entries) > maxLogLines {
			entries = entries[len(entries)-maxLogLines:]
		}
		logBufferMu.Lock()
		logBuffer = entries
		logBufferMu.Unlock()
	}
}

// ============ 追加 ============

func AppendLog(msg string) {
	now := time.Now()
	line := fmt.Sprintf("[%s] %s", now.Format("15:04:05"), msg)

	logBufferMu.Lock()
	logBuffer = append(logBuffer, logEntry{ts: now, line: line})

	cutoff := now.Add(-logRetentionDuration)
	drop := 0
	for drop < len(logBuffer) && logBuffer[drop].ts.Before(cutoff) {
		drop++
	}
	if drop > 0 {
		logBuffer = logBuffer[drop:]
	}
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
	needRewrite := appendCounter >= logRewriteEvery
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
	cutoff := time.Now().Add(-logRetentionDuration)
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		ts := parseLogTimestamp(line)
		if ts.IsZero() || ts.Before(cutoff) {
			continue
		}
		kept = append(kept, line)
	}
	if len(kept) > maxLogLines {
		kept = kept[len(kept)-maxLogLines:]
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(strings.Join(kept, "\n")+"\n"), 0644); err != nil {
		return
	}
	_ = os.Rename(tmpPath, path)
}

// ============ 读取 ============

func GetLogs() string {
	logFileMu.Lock()
	path := logFilePath
	logFileMu.Unlock()

	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			content := strings.TrimRight(string(data), "\n")
			if content != "" {
				lines := strings.Split(content, "\n")
				cutoff := time.Now().Add(-logRetentionDuration)
				entries := make([]logEntry, 0, len(lines))
				for _, line := range lines {
					ts := parseLogTimestamp(line)
					if ts.IsZero() || ts.Before(cutoff) {
						continue
					}
					entries = append(entries, logEntry{ts: ts, line: line})
				}
				if len(entries) > maxLogLines {
					entries = entries[len(entries)-maxLogLines:]
				}
				logBufferMu.Lock()
				logBuffer = entries
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
	for i, e := range logBuffer {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(e.line)
	}
	return sb.String()
}

// ============ 清空 ============

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

// ============ 标准库 log 重定向 ============

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

// ============ 初始化 ============

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
