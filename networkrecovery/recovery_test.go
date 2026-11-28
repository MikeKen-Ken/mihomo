package networkrecovery

import (
	"errors"
	"testing"
	"time"
)

type fakeActions struct {
	dnsResets   int
	routeResets int
	routeError  error
}

func (f *fakeActions) resetDNS() {
	f.dnsResets++
}

func (f *fakeActions) resetRoute() (int, error) {
	f.routeResets++
	return 4, f.routeError
}

func TestDNSFailureEscalatesAfterSoftRecovery(t *testing.T) {
	now := time.Unix(1_000, 0)
	actions := &fakeActions{}
	manager := newManager(actions, func() time.Time { return now })

	first := manager.Recover(Request{Kind: KindDNSFailure})
	if first.Action != "dns-reset" || actions.dnsResets != 1 {
		t.Fatalf("first recovery = %#v, dns resets = %d", first, actions.dnsResets)
	}

	now = now.Add(15 * time.Second)
	second := manager.Recover(Request{Kind: KindDNSFailure})
	if second.Action != "route-reset" || actions.routeResets != 1 || second.ResetAdapters != 4 {
		t.Fatalf("second recovery = %#v, route resets = %d", second, actions.routeResets)
	}
}

func TestPersistentDNSFailureRecommendsProcessRestart(t *testing.T) {
	now := time.Unix(1_500, 0)
	actions := &fakeActions{}
	manager := newManager(actions, func() time.Time { return now })

	manager.Recover(Request{Kind: KindDNSFailure})
	now = now.Add(15 * time.Second)
	manager.Recover(Request{Kind: KindDNSFailure})
	now = now.Add(15 * time.Second)
	report := manager.Recover(Request{Kind: KindDNSFailure})
	if !report.RestartRecommended || report.Action != "route-reset" {
		t.Fatalf("persistent failure recovery = %#v", report)
	}
	if status := manager.Status(); status.Sequence != report.Sequence || !status.RestartRecommended {
		t.Fatalf("recovery status = %#v", status)
	}
	manager.MarkHealthy()
	if status := manager.Status(); status.RestartRecommended || status.Action != "healthy" {
		t.Fatalf("healthy recovery status = %#v", status)
	}
}

func TestHealthyResultStartsANewRecoveryCycle(t *testing.T) {
	now := time.Unix(2_000, 0)
	actions := &fakeActions{}
	manager := newManager(actions, func() time.Time { return now })

	manager.Recover(Request{Kind: KindDNSFailure})
	manager.MarkHealthy()
	now = now.Add(time.Second)
	report := manager.Recover(Request{Kind: KindDNSFailure})
	if report.Action != "dns-reset" || actions.routeResets != 0 {
		t.Fatalf("recovery after healthy result = %#v", report)
	}
}

func TestFullRecoveryIsDebouncedAndReportsErrors(t *testing.T) {
	now := time.Unix(3_000, 0)
	actions := &fakeActions{routeError: errors.New("fake-ip flush failed")}
	manager := newManager(actions, func() time.Time { return now })

	first := manager.Recover(Request{Kind: KindRouteChanged})
	if first.Error != "fake-ip flush failed" || !first.ClosedConnections {
		t.Fatalf("first route recovery = %#v", first)
	}

	now = now.Add(500 * time.Millisecond)
	second := manager.Recover(Request{Kind: KindRouteChanged})
	if !second.Coalesced || second.ClosedConnections || actions.routeResets != 1 {
		t.Fatalf("debounced route recovery = %#v, route resets = %d", second, actions.routeResets)
	}
}

func TestDNSOnlyChangePreservesEstablishedConnections(t *testing.T) {
	actions := &fakeActions{}
	m := newManager(actions, time.Now)
	r := m.Recover(Request{Kind: KindDNSChanged})
	if r.ClosedConnections || actions.routeResets != 0 || actions.dnsResets != 1 {
		t.Fatalf("DNS change disrupted routes: %#v", r)
	}
}

func TestWorkingTrafficPreventsDNSFailureFromClosingTunnels(t *testing.T) {
	now := time.Unix(4000, 0)
	actions := &fakeActions{}
	m := newManager(actions, func() time.Time { return now })
	m.MarkTrafficHealthy()
	for i := 0; i < 3; i++ {
		now = now.Add(5 * time.Second)
		r := m.Recover(Request{Kind: KindDNSFailure})
		if r.ClosedConnections || r.RestartRecommended {
			t.Fatalf("working traffic was interrupted: %#v", r)
		}
	}
	now = now.Add(31 * time.Second)
	if !m.Recover(Request{Kind: KindDNSFailure}).ClosedConnections {
		t.Fatal("stale success prevented real recovery")
	}
}

func TestTrafficReadNeverWaitsForRecoveryLock(t *testing.T) {
	m := newManager(&fakeActions{}, time.Now)
	m.mu.Lock()
	defer m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.MarkTrafficHealthy(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("application read waited for recovery's connection-close lock")
	}
}
