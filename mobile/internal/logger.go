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
)

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
