package proxy

import (
	"strings"
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestNormalizeConfigSupportsSocks5hAlias(t *testing.T) {
	cfg, err := NormalizeConfig(connection.ProxyConfig{
		Type: "SOCKS5H",
		Host: "127.0.0.1",
		Port: 1080,
	})
	if err != nil {
		t.Fatalf("NormalizeConfig returned error: %v", err)
	}
	if cfg.Type != "socks5" {
		t.Fatalf("expected normalized proxy type socks5, got %s", cfg.Type)
	}
}

func TestForwarderCacheKeyIncludesCredentialFingerprint(t *testing.T) {
	base := connection.ProxyConfig{
		Type:     "socks5",
		Host:     "127.0.0.1",
		Port:     1080,
		User:     "tester",
		Password: "first-password",
	}
	other := base
	other.Password = "second-password"

	keyA := forwarderCacheKey(base, "db.internal", 3306)
	keyB := forwarderCacheKey(other, "db.internal", 3306)

	if keyA == keyB {
		t.Fatalf("expected different cache key for different credentials")
	}
	if strings.Contains(keyA, base.Password) || strings.Contains(keyB, other.Password) {
		t.Fatalf("cache key should not contain raw password")
	}
}
