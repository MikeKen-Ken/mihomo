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

const testGroupURL = "https://www.gstatic.com/generate_204"

type groupMemberProxy struct {
	C.Proxy

	name     string
	alive    atomic.Bool
	delay    uint16
	urlErr   error
	urlDelay uint16
	urlCalls atomic.Int32
	dialErr  error

	urlStartedOnce sync.Once
	urlStarted     chan struct{}
	urlRelease     chan struct{}
}

type recordingManualSelectionPersistence struct {
	cleared atomic.Int32
}

func (p *recordingManualSelectionPersistence) Clear(string) {
	p.cleared.Add(1)
}

func (p *groupMemberProxy) Name() string { return p.name }

func (p *groupMemberProxy) Type() C.AdapterType { return C.Shadowsocks }

func (p *groupMemberProxy) AliveForTestUrl(string) bool { return p.alive.Load() }

func (p *groupMemberProxy) LastDelayForTestUrl(string) uint16 { return p.delay }

func (p *groupMemberProxy) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, p.dialErr
}

func (p *groupMemberProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	p.urlCalls.Add(1)
	if p.urlStarted != nil {
		p.urlStartedOnce.Do(func() {
			close(p.urlStarted)
		})
	}
	if p.urlRelease != nil {
		select {
		case <-p.urlRelease:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return p.urlDelay, p.urlErr
}

func newTestFallback(maxFailedTimes int, members ...C.Proxy) *Fallback {
	gb := NewGroupBase(GroupBaseOption{
		Name:                 "Auto",
		Type:                 C.Fallback,
		TestTimeout:          1000,
		FailureResetInterval: 5000,
		MaxFailedTimes:       maxFailedTimes,
	})
	gb.selectionPersistence = nil
	gb.providerProxies = members
	return &Fallback{
		GroupBase:      gb,
		testUrl:        testGroupURL,
		expectedStatus: "204",
	}
}

func TestMaxFailedTimesOneDialFailureTriggersHealthCheck(t *testing.T) {
	current := &groupMemberProxy{name: "node-a", delay: 40, urlDelay: 40, urlErr: errors.New("generate_204 failed")}
	current.alive.Store(true)
	next := &groupMemberProxy{name: "node-b", delay: 80, urlDelay: 80}
	next.alive.Store(true)
	f := newTestFallback(1, current, next)

	var healthChecks atomic.Int32
	f.onDialFailed(context.Background(), C.Shadowsocks, errors.New("dial timeout"), current, f.testUrl, f.expectedStatus, func() {
		healthChecks.Add(1)
		current.alive.Store(false)
	})

	waitForCondition(t, time.Second, func() bool {
		return !f.connectTesting.Load()
	})

	if got := healthChecks.Load(); got != 1 {
		t.Fatalf("health check callbacks = %d, want 1 after max-failed-times=1 dial failure", got)
	}
	if got := f.Now(); got != "node-b" {
		t.Fatalf("Now() = %q, want node-b after the failed node is marked dead", got)
	}
}

func TestMaxFailedTimesReleasesPinWhenGenerate204StillWorks(t *testing.T) {
	current := &groupMemberProxy{name: "node-a", delay: 40, urlDelay: 40, dialErr: errors.New("dial timeout")}
	current.alive.Store(true)
	next := &groupMemberProxy{name: "node-b", delay: 80, urlDelay: 80}
	next.alive.Store(true)
	f := newTestFallback(1, current, next)
	persistence := &recordingManualSelectionPersistence{}
	f.selectionPersistence = persistence
	f.ForceSet("node-a")

	if _, err := f.DialContext(context.Background(), nil); err == nil {
		t.Fatal("DialContext error = nil, want dial timeout")
	}

	waitForCondition(t, time.Second, func() bool {
		return !f.connectTesting.Load()
	})

	if f.NowIsManual() {
		t.Fatal("manual selection remained after max-failed-times was reached")
	}
	if got := persistence.cleared.Load(); got != 1 {
		t.Fatalf("persisted selection clears = %d, want 1", got)
	}
	if got := f.Now(); got != "node-a" {
		t.Fatalf("Now() = %q, want node-a to remain eligible for automatic selection", got)
	}
}

func TestStaleFallbackPrecheckDoesNotClearNewerSelection(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	old := &groupMemberProxy{
		name:       "node-a",
		delay:      40,
		urlErr:     errors.New("generate_204 failed"),
		dialErr:    errors.New("dial timeout"),
		urlStarted: started,
		urlRelease: release,
	}
	old.alive.Store(true)
	newer := &groupMemberProxy{name: "node-b", delay: 80}
	newer.alive.Store(true)
	f := newTestFallback(1, old, newer)
	persistence := &recordingManualSelectionPersistence{}
	f.selectionPersistence = persistence
	f.ForceSet("node-a")

	if _, err := f.DialContext(context.Background(), nil); err == nil {
		t.Fatal("DialContext error = nil, want dial timeout")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("max-failed-times precheck did not start")
	}

	f.ForceSet("node-b")
	close(release)
	waitForCondition(t, time.Second, func() bool {
		return !f.connectTesting.Load()
	})

	if !f.NowIsManual() {
		t.Fatal("newer manual selection was cleared by a stale precheck")
	}
	if got := f.Now(); got != "node-b" {
		t.Fatalf("Now() = %q, want newer manual selection node-b", got)
	}
	if got := persistence.cleared.Load(); got != 0 {
		t.Fatalf("persisted selection clears = %d, want 0 for a stale precheck", got)
	}
}

func TestFallbackHealthCheckUnpinsFailedNode(t *testing.T) {
	dead := &groupMemberProxy{name: "node-a", delay: 0xffff}
	dead.alive.Store(false)
	alive := &groupMemberProxy{name: "node-b", delay: 80}
	alive.alive.Store(true)
	f := newTestFallback(1, dead, alive)
	persistence := &recordingManualSelectionPersistence{}
	f.selectionPersistence = persistence
	f.ForceSet("node-a")
	selection := f.selection.snapshot()

	if got := f.Now(); got != "node-a" {
		t.Fatalf("pinned Now() = %q, want node-a", got)
	}

	f.healthCheckForProxy(dead, selection)

	if f.NowIsManual() {
		t.Fatal("pin remained after max-failed-times health check")
	}
	if got := f.Now(); got != "node-b" {
		t.Fatalf("Now() after health check = %q, want node-b", got)
	}
	if got := persistence.cleared.Load(); got != 1 {
		t.Fatalf("persisted selection clears = %d, want 1", got)
	}
}
