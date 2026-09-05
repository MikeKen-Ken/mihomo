package adapter

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
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

type delayedDialAdapter struct {
	*outbound.Base
	dialDelay time.Duration
}

func (a *delayedDialAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if a.dialDelay > 0 {
		timer := time.NewTimer(a.dialDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	conn, err := net.Dial("tcp", metadata.RemoteAddress())
	if err != nil {
		return nil, err
	}
	return outbound.NewConn(conn, a), nil
}

func startTwoShotHEADServer(t *testing.T, firstDelay, secondDelay time.Duration) *httptest.Server {
	t.Helper()
	var n atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delay := firstDelay
		if n.Add(1) > 1 {
			delay = secondDelay
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	return server
}

func runURLDelayTest(t *testing.T, timeoutMs int, dialDelay, firstHTTP, secondHTTP time.Duration) (uint16, error) {
	t.Helper()
	prevHome := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(prevHome) })

	server := startTwoShotHEADServer(t, firstHTTP, secondHTTP)
	proxy := NewProxy(&delayedDialAdapter{
		Base:      outbound.NewBase(outbound.BaseOption{Name: "delay-node", Type: C.Direct}),
		dialDelay: dialDelay,
	})
	ctx := C.WithDelayTestTimeoutMs(context.Background(), timeoutMs)
	return proxy.URLTest(ctx, server.URL, nil)
}

func TestURLTestDelayIsConnectPlusFirstHTTP(t *testing.T) {
	const timeoutMs = 200
	delay, err := runURLDelayTest(t, timeoutMs, 0, 40*time.Millisecond, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("URLTest() error = %v; 40ms connect+HTTP must pass a 200ms timeout", err)
	}
	if delay < 20 || delay >= uint16(timeoutMs) {
		t.Fatalf("URLTest() delay = %dms; want first HTTP around 40ms, not the timeout value", delay)
	}
}

func TestURLTestTimesOutOnTotalConnectTime(t *testing.T) {
	const timeoutMs = 80
	delay, err := runURLDelayTest(t, timeoutMs, 0, 120*time.Millisecond, 20*time.Millisecond)
	if err == nil {
		t.Fatalf("URLTest() delay = %dms, error = nil; 120ms connect+HTTP must fail an 80ms timeout", delay)
	}
}

func TestURLTestIgnoresUnifiedDelaySecondRequest(t *testing.T) {
	prev := UnifiedDelay.Load()
	UnifiedDelay.Store(true)
	t.Cleanup(func() { UnifiedDelay.Store(prev) })

	const timeoutMs = 300
	delay, err := runURLDelayTest(t, timeoutMs, 0, 40*time.Millisecond, 180*time.Millisecond)
	if err != nil {
		t.Fatalf("URLTest() error = %v; unified-delay must not switch the clock to the second HEAD", err)
	}
	if delay > 90 {
		t.Fatalf("URLTest() delay = %dms; want first HTTP around 40ms, not second HEAD around 180ms", delay)
	}
}

func TestURLTestSharesOneDeadlineAcrossDialAndHTTP(t *testing.T) {
	const timeoutMs = 100
	delay, err := runURLDelayTest(t, timeoutMs, 70*time.Millisecond, 70*time.Millisecond, 20*time.Millisecond)
	if err == nil {
		t.Fatalf("URLTest() delay = %dms, error = nil; dial 70ms + HTTP 70ms must share one 100ms deadline", delay)
	}
}
