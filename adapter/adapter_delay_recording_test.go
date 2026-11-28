package adapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

type failingDelayAdapter struct{ *outbound.Base }

func (a *failingDelayAdapter) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, errors.New("test dial failure")
}

func TestURLTestRecordsBeforeOwnContextCleanup(t *testing.T) {
	previous := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previous) })
	server := startTwoShotHEADServer(t, 10*time.Millisecond, 10*time.Millisecond)

	for _, success := range []bool{true, false} {
		name := "failure"
		if success {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			base := outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct})
			var adapter C.ProxyAdapter = &failingDelayAdapter{base}
			if success {
				adapter = &delayedDialAdapter{Base: base}
			}
			proxy := NewProxy(adapter)
			ctx := C.WithDelayTestTimeoutMs(context.Background(), 1000)
			delay, err := proxy.URLTest(ctx, server.URL, nil)
			if (err == nil) != success {
				t.Fatalf("unexpected test outcome: delay=%d err=%v", delay, err)
			}
			for _, history := range [][]C.DelayHistory{proxy.DelayHistory(), proxy.DelayHistoryForTestUrl(server.URL)} {
				if len(history) != 1 {
					t.Fatalf("completed test recorded %d history entries; want 1", len(history))
				}
				if (history[0].Delay > 0) != success {
					t.Fatalf("history does not reflect the test result: %+v", history)
				}
			}
			if proxy.AliveForTestUrl(server.URL) != success {
				t.Fatal("URL liveness does not reflect the completed test")
			}
		})
	}
}

func TestURLTestExternalCancellationDoesNotRecordFailure(t *testing.T) {
	proxy := NewProxy(&failingDelayAdapter{outbound.NewBase(outbound.BaseOption{Name: "cancelled", Type: C.Direct})})
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = proxy.URLTest(C.WithDelayTestTimeoutMs(parent, 1000), "http://example.test", nil)
	if len(proxy.DelayHistory()) != 0 || !proxy.alive.Load() {
		t.Fatal("external cancellation changed node health")
	}
}
