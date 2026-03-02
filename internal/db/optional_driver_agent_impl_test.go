package db

import (
	"context"
	"testing"
	"time"
)

func TestTimeoutMsFromContext_NoDeadline(t *testing.T) {
	if got := timeoutMsFromContext(context.Background()); got != 0 {
		t.Fatalf("无 deadline 时应返回 0，got=%d", got)
	}
}

func TestTimeoutMsFromContext_WithDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	got := timeoutMsFromContext(ctx)
	if got <= 0 {
		t.Fatalf("有 deadline 时应返回正值，got=%d", got)
	}
}

func TestTimeoutMsFromContext_ExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	if got := timeoutMsFromContext(ctx); got != 1 {
		t.Fatalf("过期 deadline 应返回 1，got=%d", got)
	}
}
