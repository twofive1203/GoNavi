package app

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestWrapConnectError_MongoNoSSL_RemovesMisleadingSSLLabel(t *testing.T) {
	config := connection.ConnectionConfig{
		Type:   "mongodb",
		UseSSL: false,
	}
	sourceErr := errors.New("MongoDB 连接失败：SSL 主库凭据验证失败: mock error")

	wrapped := wrapConnectError(config, sourceErr)
	text := wrapped.Error()
	if strings.Contains(text, "SSL 主库凭据") {
		t.Fatalf("expected ssl label to be removed when TLS disabled, got: %s", text)
	}
	if !strings.Contains(text, "主库凭据验证失败") {
		t.Fatalf("expected auth label to remain, got: %s", text)
	}
}

func TestWrapConnectError_MongoURIForcesTLS_KeepsSSLLabel(t *testing.T) {
	config := connection.ConnectionConfig{
		Type:   "mongodb",
		UseSSL: false,
		URI:    "mongodb://user:pass@127.0.0.1:27017/admin?tls=true",
	}
	sourceErr := errors.New("MongoDB 连接失败：SSL 主库凭据验证失败: mock error")

	wrapped := wrapConnectError(config, sourceErr)
	text := wrapped.Error()
	if !strings.Contains(text, "SSL 主库凭据") {
		t.Fatalf("expected ssl label to remain when URI enables TLS, got: %s", text)
	}
}

func TestWrapConnectError_MongoSRVDefaultTLS_KeepsSSLLabel(t *testing.T) {
	config := connection.ConnectionConfig{
		Type:   "mongodb",
		UseSSL: false,
		URI:    "mongodb+srv://user:pass@cluster0.example.com/admin",
	}
	sourceErr := errors.New("MongoDB 连接失败：SSL 主库凭据验证失败: mock error")

	wrapped := wrapConnectError(config, sourceErr)
	text := wrapped.Error()
	if !strings.Contains(text, "SSL 主库凭据") {
		t.Fatalf("expected ssl label to remain for mongodb+srv default TLS, got: %s", text)
	}
}

func TestWithLogHintError_OmitEmptyLogPath(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "gonavi.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatalf("write empty log failed: %v", err)
	}
	err := withLogHint{err: errors.New("连接失败"), logPath: logPath}
	text := err.Error()
	if strings.Contains(text, "详细日志：") {
		t.Fatalf("expected no log hint for empty file, got: %s", text)
	}
}

func TestWithLogHintError_IncludeNonEmptyLogPath(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "gonavi.log")
	if err := os.WriteFile(logPath, []byte("log entry\n"), 0o644); err != nil {
		t.Fatalf("write log failed: %v", err)
	}
	err := withLogHint{err: errors.New("连接失败"), logPath: logPath}
	text := err.Error()
	if !strings.Contains(text, "详细日志："+logPath) {
		t.Fatalf("expected log hint with path, got: %s", text)
	}
}
