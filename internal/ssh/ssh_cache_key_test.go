package ssh

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestNewSSHClientCacheKey_DiffPassword(t *testing.T) {
	a := newSSHClientCacheKey(connection.SSHConfig{
		Host:     "127.0.0.1",
		Port:     22,
		User:     "root",
		Password: "a",
	})
	b := newSSHClientCacheKey(connection.SSHConfig{
		Host:     "127.0.0.1",
		Port:     22,
		User:     "root",
		Password: "b",
	})
	if a == b {
		t.Fatalf("expected different cache key when password differs")
	}
	if a.host != b.host || a.port != b.port || a.user != b.user {
		t.Fatalf("expected host/port/user to stay identical")
	}
}

func TestNewSSHClientCacheKey_DiffKeyPath(t *testing.T) {
	a := newSSHClientCacheKey(connection.SSHConfig{
		Host:    "127.0.0.1",
		Port:    22,
		User:    "root",
		KeyPath: "/tmp/a.key",
	})
	b := newSSHClientCacheKey(connection.SSHConfig{
		Host:    "127.0.0.1",
		Port:    22,
		User:    "root",
		KeyPath: "/tmp/b.key",
	})
	if a == b {
		t.Fatalf("expected different cache key when keyPath differs")
	}
}
