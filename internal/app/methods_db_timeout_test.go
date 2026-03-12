package app

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestNormalizeTestConnectionConfig_DefaultToUpperBound(t *testing.T) {
	config := connection.ConnectionConfig{Type: "mongodb", Timeout: 0}
	got := normalizeTestConnectionConfig(config)
	if got.Timeout != testConnectionTimeoutUpperBoundSeconds {
		t.Fatalf("expected timeout=%d, got=%d", testConnectionTimeoutUpperBoundSeconds, got.Timeout)
	}
}

func TestNormalizeTestConnectionConfig_KeepSmallerTimeout(t *testing.T) {
	config := connection.ConnectionConfig{Type: "mongodb", Timeout: 6}
	got := normalizeTestConnectionConfig(config)
	if got.Timeout != 6 {
		t.Fatalf("expected timeout=6, got=%d", got.Timeout)
	}
}

func TestNormalizeTestConnectionConfig_ClampLargeTimeout(t *testing.T) {
	config := connection.ConnectionConfig{Type: "mongodb", Timeout: 60}
	got := normalizeTestConnectionConfig(config)
	if got.Timeout != testConnectionTimeoutUpperBoundSeconds {
		t.Fatalf("expected timeout=%d, got=%d", testConnectionTimeoutUpperBoundSeconds, got.Timeout)
	}
}
