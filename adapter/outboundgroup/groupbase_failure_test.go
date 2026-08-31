package outboundgroup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

type blockingPrecheckProxy struct {
	C.Proxy

	urlTestCalls atomic.Int32
	startedOnce  sync.Once
	started      chan struct{}
	release      chan struct{}
}

func (p *blockingPrecheckProxy) Name() string {
	return "test-proxy"
}

func (p *blockingPrecheckProxy) Type() C.AdapterType {
	return C.Shadowsocks
}

func (p *blockingPrecheckProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	p.urlTestCalls.Add(1)
	p.startedOnce.Do(func() {
		close(p.started)
	})

	select {
	case <-p.release:
		return 1, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestConcurrentDialFailuresStartOnePrecheck(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{
		Name:                 "test-group",
		Type:                 C.URLTest,
		TestTimeout:          1000,
		FailureResetInterval: 5000,
		MaxFailedTimes:       1,
	})
	proxy := &blockingPrecheckProxy{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	dialErr := errors.New("test dial failure")

	const initialFailures = 32
	var initial sync.WaitGroup
	initial.Add(initialFailures)
	for range initialFailures {
		go func() {
			defer initial.Done()
			gb.onDialFailed(context.Background(), C.Shadowsocks, dialErr, proxy, "https://example.com", "204", func() {})
		}()
	}
	initial.Wait()

	select {
	case <-proxy.started:
	case <-time.After(time.Second):
		t.Fatal("precheck did not start")
	}

	if got := proxy.urlTestCalls.Load(); got != 1 {
		t.Fatalf("URLTest calls = %d, want 1", got)
	}

	gb.failedTestMux.Lock()
	countWhileTesting := gb.failedTimes
	gb.failedTestMux.Unlock()
	if countWhileTesting != initialFailures {
		t.Fatalf("failure count before additional traffic = %d, want %d", countWhileTesting, initialFailures)
	}

	const failuresDuringPrecheck = 32
	var during sync.WaitGroup
	during.Add(failuresDuringPrecheck)
	for range failuresDuringPrecheck {
		go func() {
			defer during.Done()
			gb.onDialFailed(context.Background(), C.Shadowsocks, dialErr, proxy, "https://example.com", "204", func() {})
		}()
	}
	during.Wait()

	gb.failedTestMux.Lock()
	gotCount := gb.failedTimes
	gb.failedTestMux.Unlock()
	wantCount := countWhileTesting + failuresDuringPrecheck
	if gotCount != wantCount {
		t.Fatalf("live failure count during precheck = %d, want %d", gotCount, wantCount)
	}

	close(proxy.release)
	waitForCondition(t, time.Second, func() bool {
		return !gb.connectTesting.Load()
	})

	if got := proxy.urlTestCalls.Load(); got != 1 {
		t.Fatalf("URLTest calls after precheck = %d, want 1", got)
	}
	gb.failedTestMux.Lock()
	defer gb.failedTestMux.Unlock()
	if gb.failedTimes != 0 {
		t.Fatalf("failure count after successful precheck = %d, want 0", gb.failedTimes)
	}
	if !gb.failedTime.IsZero() {
		t.Fatalf("failure timestamp after successful precheck = %v, want zero", gb.failedTime)
	}
}

func TestFailureCounterConcurrentReset(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{
		Name:                 "test-group",
		Type:                 C.URLTest,
		FailureResetInterval: 5000,
		MaxFailedTimes:       100000,
	})
	dialErr := errors.New("test dial failure")

	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(iterations * 2)
	for range iterations {
		go func() {
			defer wg.Done()
			gb.onDialFailed(context.Background(), C.Shadowsocks, dialErr, nil, "", "", nil)
		}()
		go func() {
			defer wg.Done()
			gb.onDialSuccess()
		}()
	}
	wg.Wait()

	gb.onDialSuccess()
	gb.failedTestMux.Lock()
	defer gb.failedTestMux.Unlock()
	if gb.failedTimes != 0 {
		t.Fatalf("failure count after final reset = %d, want 0", gb.failedTimes)
	}
	if !gb.failedTime.IsZero() {
		t.Fatalf("failure timestamp after final reset = %v, want zero", gb.failedTime)
	}
}

func TestDialFailureBookkeepingCompletesBeforeReturn(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{
		Name:                 "test-group",
		Type:                 C.URLTest,
		FailureResetInterval: 5000,
		MaxFailedTimes:       100,
	})

	gb.onDialFailed(context.Background(), C.Shadowsocks, errors.New("test dial failure"), nil, "", "", nil)

	gb.failedTestMux.Lock()
	failedTimes := gb.failedTimes
	gb.failedTestMux.Unlock()
	if failedTimes != 1 {
		t.Fatalf("failure count when onDialFailed returned = %d, want 1", failedTimes)
	}

	gb.onDialSuccess()
	gb.failedTestMux.Lock()
	defer gb.failedTestMux.Unlock()
	if gb.failedTimes != 0 {
		t.Fatalf("failure count after later success = %d, want 0", gb.failedTimes)
	}
	if !gb.failedTime.IsZero() {
		t.Fatalf("failure timestamp after later success = %v, want zero", gb.failedTime)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
