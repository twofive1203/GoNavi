package logger

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	envLogDir     = "GONAVI_LOG_DIR"
	appHiddenDir  = ".GoNavi"
	appLogDirName = "Logs"

	logFileName         = "gonavi.log"
	logRotateMaxBytes   = 10 * 1024 * 1024 // 10MB
	logRotateMaxBackups = 10
)

var (
	once    sync.Once
	logMu   sync.Mutex
	logInst *log.Logger
	logFile *os.File
	logPath string
)

func Init() {
	once.Do(func() {
		path, out := initOutput()
		logMu.Lock()
		defer logMu.Unlock()
		logPath = path
		logInst = log.New(out, "", log.Ldate|log.Ltime|log.Lmicroseconds)
		logInst.Printf("[INFO] 日志初始化完成，日志文件：%s", logPath)
	})
}

func Path() string {
	Init()
	logMu.Lock()
	defer logMu.Unlock()
	return logPath
}

func Close() {
	Init()
	logMu.Lock()
	defer logMu.Unlock()
	if logInst != nil {
		logInst.SetOutput(os.Stderr)
	}
	if logFile != nil {
		_ = logFile.Close()
		logFile = nil
	}
}

func Infof(format string, args ...any) {
	printf("INFO", format, args...)
}

func Warnf(format string, args ...any) {
	printf("WARN", format, args...)
}

func Errorf(format string, args ...any) {
	printf("ERROR", format, args...)
}

func Error(err error, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if err == nil {
		Errorf("%s", msg)
		return
	}
	Errorf("%s；错误链：%s", msg, ErrorChain(err))
}

func ErrorChain(err error) string {
	if err == nil {
		return ""
	}

	var parts []string
	seen := map[string]struct{}{}
	cur := err
	truncated := false
	for i := 0; cur != nil && i < 20; i++ {
		s := cur.Error()
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			parts = append(parts, s)
		}
		cur = errors.Unwrap(cur)
	}
	if cur != nil {
		truncated = true
	}

	if len(parts) == 0 {
		return err.Error()
	}
	if truncated {
		parts = append(parts, "（错误链过长，已截断）")
	}
	return strings.Join(parts, " -> ")
}

func printf(level string, format string, args ...any) {
	Init()
	logMu.Lock()
	defer logMu.Unlock()
	inst := logInst
	if inst == nil {
		return
	}
	inst.Printf("[%s] %s", level, fmt.Sprintf(format, args...))
	if logFile != nil {
		_ = logFile.Sync()
	}
}

func initOutput() (string, io.Writer) {
	dir := strings.TrimSpace(os.Getenv(envLogDir))
	if dir == "" {
		dir = defaultLogDir()
	}

	if path, writer, ok := openLogFile(dir); ok {
		return path, writer
	}

	fallbackDir := filepath.Join(os.TempDir(), appHiddenDir, appLogDirName)
	if path, writer, ok := openLogFile(fallbackDir); ok {
		return path, writer
	}

	return "", os.Stderr
}

func defaultLogDir() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return filepath.Join(os.TempDir(), appHiddenDir, appLogDirName)
	}
	return filepath.Join(home, appHiddenDir, appLogDirName)
}

func openLogFile(dir string) (string, io.Writer, bool) {
	if strings.TrimSpace(dir) == "" {
		return "", nil, false
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, false
	}
	path := filepath.Join(dir, logFileName)
	rotateIfNeeded(path, dir)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return "", nil, false
	}
	logFile = f
	return path, f, true
}

func rotateIfNeeded(path, dir string) {
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() {
		return
	}
	if fi.Size() < logRotateMaxBytes {
		return
	}

	ts := time.Now().Format("20060102-150405")
	rotated := filepath.Join(dir, fmt.Sprintf("gonavi-%s.log", ts))
	if err := os.Rename(path, rotated); err != nil {
		return
	}
	cleanupOldLogs(dir)
}

func cleanupOldLogs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	type item struct {
		name string
		path string
	}
	var logs []item
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "gonavi-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		logs = append(logs, item{name: name, path: filepath.Join(dir, name)})
	}

	sort.Slice(logs, func(i, j int) bool { return logs[i].name > logs[j].name })
	if len(logs) <= logRotateMaxBackups {
		return
	}
	for _, it := range logs[logRotateMaxBackups:] {
		_ = os.Remove(it.path)
	}
}
