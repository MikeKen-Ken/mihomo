package constant

import (
	"context"
	"testing"
)

func TestDelayTestTimeoutMs(t *testing.T) {
	ctx := WithDelayTestTimeoutMs(context.Background(), 300)
	timeoutMs, ok := DelayTestTimeoutMs(ctx)
	if !ok || timeoutMs != 300 {
		t.Fatalf("DelayTestTimeoutMs() = %d, %t; want 300, true", timeoutMs, ok)
	}
}

func TestDelayTestTimeoutMsRejectsNonPositiveValue(t *testing.T) {
	ctx := WithDelayTestTimeoutMs(context.Background(), 0)
	if timeoutMs, ok := DelayTestTimeoutMs(ctx); ok {
		t.Fatalf("DelayTestTimeoutMs() = %d, true; want unset", timeoutMs)
	}
}
