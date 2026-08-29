package outbound

import (
	"context"
	"sync"
	"testing"

	"golang.org/x/sync/semaphore"
)

func TestOpenVPNConcurrentResetAndClose(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())
	adapter := &OpenVPN{
		runCtx:    runCtx,
		runCancel: runCancel,
		runLock:   semaphore.NewWeighted(1),
	}

	var workers sync.WaitGroup
	for range 16 {
		workers.Add(2)
		go func() {
			defer workers.Done()
			if err := adapter.ResetNetworkState(); err != nil {
				t.Errorf("reset network state: %v", err)
			}
		}()
		go func() {
			defer workers.Done()
			if err := adapter.Close(); err != nil {
				t.Errorf("close adapter: %v", err)
			}
		}()
	}
	workers.Wait()

	current, _ := adapter.runtimeContext()
	if current.Err() == nil {
		t.Fatal("runtime context remains active after close")
	}
}
