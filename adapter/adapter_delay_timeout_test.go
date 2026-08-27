package adapter

import (
	"context"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func TestDelayReachedTimeout(t *testing.T) {
	if delayReachedTimeout(299, 300) {
		t.Fatal("delay below timeout must remain successful")
	}
	if !delayReachedTimeout(300, 300) {
		t.Fatal("delay equal to timeout must fail")
	}
	if !delayReachedTimeout(301, 300) {
		t.Fatal("delay above timeout must fail")
	}
}

func TestExplicitDelayTimeoutCreatesFreshPhaseDeadline(t *testing.T) {
	ctx := C.WithDelayTestTimeoutMs(context.Background(), 300)
	first, cancelFirst := delayTestPhaseContext(ctx, 300)
	defer cancelFirst()
	firstDeadline, ok := first.Deadline()
	if !ok {
		t.Fatal("first phase must have a deadline")
	}

	time.Sleep(10 * time.Millisecond)
	second, cancelSecond := delayTestPhaseContext(ctx, 300)
	defer cancelSecond()
	secondDeadline, ok := second.Deadline()
	if !ok {
		t.Fatal("second phase must have a deadline")
	}
	if !secondDeadline.After(firstDeadline) {
		t.Fatal("each unified-delay phase must receive a fresh timeout window")
	}
}
