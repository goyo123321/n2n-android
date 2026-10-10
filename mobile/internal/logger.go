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

// maxLogLines 内存缓冲 + 文件保留的最大行数。
//
// 日志现在很干净（P2P 稳定时几乎无输出），1000 行足够回顾最近的一次
// 打洞/连接事件。超出后丢弃最旧的行。
const maxLogLines = 1000

// rewriteEvery 每 N 次追加后重写一次文件。
//
// 写入用 O_APPEND 性能好，但文件会无限增长。每 N 次追加后重写，
// 只保留最近 maxLogLines 行，让文件大小稳定。
const rewriteEvery = 1000

// ============ 内部状态 ============

var (
	logBuffer   []string
	logBufferMu sync.RWMutex

	logFilePath string
	logFileMu   sync.Mutex

	appendCounter int
	counterMu     sync.Mutex
)

// ============ 文件路径设置 ============

func setLogFilePath(path string) {
	logFileMu.Lock()
	logFilePath = path
	logFileMu.Unlock()

	if path == "" {
		return
	}

	// 读回历史日志（截断到 maxLogLines）
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

// ============ 追加 ============

// AppendLog 追加一行日志（带时间戳），同步写文件。
//
// 淘汰策略：
//   - 内存：超过 maxLogLines 时丢弃最旧的
//   - 文件：每 rewriteEvery 次追加重写一次，只保留最近 maxLogLines 行
func AppendLog(msg string) {
	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)

	// 更新内存缓冲
	logBufferMu.Lock()
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > maxLogLines {
		logBuffer = logBuffer[len(logBuffer)-maxLogLines:]
	}
	logBufferMu.Unlock()

	// 写文件（追加模式）
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

	// 定期重写，限制文件大小
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

// rewriteLogFile 重写文件，只保留最近 maxLogLines 行。
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

	// 原子写：先写临时文件再 rename
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return
	}
	_ = os.Rename(tmpPath, path)
}

// ============ 读取 ============

// GetLogs 每次从文件重读（这样 Kotlin ktLog 写的内容也能被看到）。
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
