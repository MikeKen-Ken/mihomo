package outboundgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/singledo"
	C "github.com/metacubex/mihomo/constant"
)

func newFailedTimesURLTest(maxFailedTimes int, members ...C.Proxy) *URLTest {
	gb := NewGroupBase(GroupBaseOption{
		Name:                 "Auto",
		Type:                 C.URLTest,
		TestTimeout:          1000,
		FailureResetInterval: 5000,
		MaxFailedTimes:       maxFailedTimes,
	})
	gb.selectionPersistence = nil
	gb.providerProxies = members
	return &URLTest{
		GroupBase:      gb,
		testUrl:        testGroupURL,
		expectedStatus: "204",
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 10),
	}
}

func TestURLTestMaxFailedTimesReleasesAndPersistsPin(t *testing.T) {
	current := &groupMemberProxy{name: "node-a", delay: 40, urlDelay: 40, dialErr: errors.New("dial timeout")}
	current.alive.Store(true)
	next := &groupMemberProxy{name: "node-b", delay: 80, urlDelay: 80}
	next.alive.Store(true)
	u := newFailedTimesURLTest(1, current, next)
	persistence := &recordingManualSelectionPersistence{}
	u.selectionPersistence = persistence
	u.ForceSet("node-a")

	if _, err := u.DialContext(context.Background(), nil); err == nil {
		t.Fatal("DialContext error = nil, want dial timeout")
	}
	waitForCondition(t, time.Second, func() bool {
		return !u.connectTesting.Load()
	})

	if u.NowIsManual() {
		t.Fatal("manual selection remained after max-failed-times was reached")
	}
	if got := persistence.cleared.Load(); got != 1 {
		t.Fatalf("persisted selection clears = %d, want 1", got)
	}
}

func TestStaleURLTestPrecheckDoesNotClearNewerSelection(t *testing.T) {
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
	u := newFailedTimesURLTest(1, old, newer)
	persistence := &recordingManualSelectionPersistence{}
	u.selectionPersistence = persistence
	u.ForceSet("node-a")

	if _, err := u.DialContext(context.Background(), nil); err == nil {
		t.Fatal("DialContext error = nil, want dial timeout")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("max-failed-times precheck did not start")
	}

	u.ForceSet("node-b")
	close(release)
	waitForCondition(t, time.Second, func() bool {
		return !u.connectTesting.Load()
	})

	if !u.NowIsManual() {
		t.Fatal("newer manual selection was cleared by a stale precheck")
	}
	if got := u.Now(); got != "node-b" {
		t.Fatalf("Now() = %q, want newer manual selection node-b", got)
	}
	if got := persistence.cleared.Load(); got != 0 {
		t.Fatalf("persisted selection clears = %d, want 0 for a stale precheck", got)
	}
}
