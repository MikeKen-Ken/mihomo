package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// A health check that loses the single-flight guard reaches no verdict about the
// pinned node at all, so it must not be the reason the pin survives. Contrast
// with TestFailedBackupCheckKeepsSelection, where the same nil candidate comes
// from a check that ran and found nothing.
func TestLostHealthCheckGuardReleasesPin(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		dead := &groupMemberProxy{name: "dead", delay: 0xffff}
		f := newTestFallback(1, dead)
		f.providers = []P.ProxyProvider{&readyTestProvider{members: []C.Proxy{dead}}}
		persistence := &recordingManualSelectionPersistence{}
		f.selectionPersistence = persistence
		f.ForceSet("dead")
		f.failedTesting.Store(true) // another recovery already owns this group

		f.healthCheckForProxy(dead, f.selection.snapshot())

		if f.NowIsManual() {
			t.Fatal("pin survived a lost health-check guard race")
		}
		if got := persistence.cleared.Load(); got != 1 {
			t.Fatalf("persisted selection clears = %d, want 1", got)
		}
	})

	t.Run("url-test", func(t *testing.T) {
		dead := &groupMemberProxy{name: "dead", delay: 0xffff}
		u := newFailedTimesURLTest(1, dead)
		u.providers = []P.ProxyProvider{&readyTestProvider{members: []C.Proxy{dead}}}
		persistence := &recordingManualSelectionPersistence{}
		u.selectionPersistence = persistence
		u.ForceSet("dead")
		u.failedTesting.Store(true)

		u.healthCheckForSelection(u.selection.snapshot())

		if u.NowIsManual() {
			t.Fatal("pin survived a lost health-check guard race")
		}
		if got := persistence.cleared.Load(); got != 1 {
			t.Fatalf("persisted selection clears = %d, want 1", got)
		}
	})
}
