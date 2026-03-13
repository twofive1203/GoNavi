package redis

import (
	"errors"
	"testing"
)

func TestSanitizeRedisPassword(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty password",
			input:    "",
			expected: "",
		},
		{
			name:     "plain password without special chars",
			input:    "mypassword123",
			expected: "mypassword123",
		},
		{
			name:     "password with @ not encoded",
			input:    "p@ssword",
			expected: "p@ssword",
		},
		{
			name:     "password with @ URL-encoded as %40",
			input:    "p%40ssword",
			expected: "p@ssword",
		},
		{
			name:     "password with multiple encoded chars",
			input:    "p%40ss%23word",
			expected: "p@ss#word",
		},
		{
			name:     "password with + encoded as %2B",
			input:    "p%2Bss",
			expected: "p+ss",
		},
		{
			name:     "password that is purely encoded",
			input:    "%40%23%24",
			expected: "@#$",
		},
		{
			name:     "password with invalid percent encoding",
			input:    "p%ZZssword",
			expected: "p%ZZssword",
		},
		{
			name:     "password with trailing percent",
			input:    "password%",
			expected: "password%",
		},
		{
			name:     "password with literal percent not encoding anything",
			input:    "100%safe",
			expected: "100%safe",
		},
		{
			name:     "password with space encoded as %20",
			input:    "my%20pass",
			expected: "my pass",
		},
		{
			name:     "complex password with mixed content",
			input:    "P%40ss%23w0rd!",
			expected: "P@ss#w0rd!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeRedisPassword(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeRedisPassword(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsRedisKeyGone(t *testing.T) {
	tests := []struct {
		name    string
		keyType string
		ttl     int64
		want    bool
	}{
		{name: "type none", keyType: "none", ttl: -2, want: true},
		{name: "type none without ttl", keyType: "none", ttl: -1, want: true},
		{name: "missing by ttl", keyType: "string", ttl: -2, want: true},
		{name: "normal string", keyType: "string", ttl: 30, want: false},
		{name: "permanent hash", keyType: "hash", ttl: -1, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRedisKeyGone(tt.keyType, tt.ttl); got != tt.want {
				t.Fatalf("isRedisKeyGone(%q, %d)=%v, want %v", tt.keyType, tt.ttl, got, tt.want)
			}
		})
	}
}

func TestNormalizeRedisGetValueError(t *testing.T) {
	err := normalizeRedisGetValueError("none", -2)
	if !errors.Is(err, ErrRedisKeyGone) {
		t.Fatalf("expected ErrRedisKeyGone, got %v", err)
	}
	if err == nil || err.Error() != "Redis Key 不存在或已过期" {
		t.Fatalf("unexpected error text: %v", err)
	}

	if normalizeRedisGetValueError("hash", -1) != nil {
		t.Fatal("expected nil for supported existing key")
	}
}
