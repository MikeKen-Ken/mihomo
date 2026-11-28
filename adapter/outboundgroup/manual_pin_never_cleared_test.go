package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// A pin is released only by an explicit ClearManualSelection call from the
// user. Failures, health checks, lost guard races, delay tests and a pinned node
// that is briefly missing from the group must all leave it in place.

type pinnedGroup struct {
	name       string
	forceSet   func(string)
	isManual   func() bool
	now        func() string
	healthTest func()
	persist    *recordingManualSelectionPersistence
}

func newPinnedFallback(members []C.Proxy, ready bool) *pinnedGroup {
	f := newTestFallback(1, members...)
	f.providers = []P.ProxyProvider{&readyTestProvider{members: members, ready: ready}}
	persist := &recordingManualSelectionPersistence{}
	f.selectionPersistence = persist
	return &pinnedGroup{
		name:       "fallback",
		forceSet:   f.ForceSet,
		isManual:   f.NowIsManual,
		now:        f.Now,
		healthTest: func() { f.healthCheckForProxy(members[0], f.selection.snapshot()) },
		persist:    persist,
	}
}

func newPinnedURLTest(members []C.Proxy, ready bool) *pinnedGroup {
	u := newFailedTimesURLTest(1, members...)
	u.providers = []P.ProxyProvider{&readyTestProvider{members: members, ready: ready}}
	persist := &recordingManualSelectionPersistence{}
	u.selectionPersistence = persist
	return &pinnedGroup{
		name:       "url-test",
		forceSet:   u.ForceSet,
		isManual:   u.NowIsManual,
		now:        u.Now,
		healthTest: func() { u.healthCheckForSelection(u.selection.snapshot()) },
		persist:    persist,
	}
}

func pinnedGroupKinds() map[string]func([]C.Proxy, bool) *pinnedGroup {
	return map[string]func([]C.Proxy, bool) *pinnedGroup{
		"fallback": newPinnedFallback,
		"url-test": newPinnedURLTest,
	}
}

func assertPinKept(t *testing.T, g *pinnedGroup, want string) {
	t.Helper()
	if !g.isManual() {
		t.Fatalf("%s: pin was released", g.name)
	}
	if got := g.now(); got != want {
		t.Fatalf("%s: Now() = %q, want pinned %q", g.name, got, want)
	}
	if got := g.persist.cleared.Load(); got != 0 {
		t.Fatalf("%s: persisted selection cleared %d times, want 0", g.name, got)
	}
}

func TestPinSurvivesHealthCheckThatFindsHealthyBackup(t *testing.T) {
	for kind, build := range pinnedGroupKinds() {
		t.Run(kind, func(t *testing.T) {
			dead := &groupMemberProxy{name: "dead", delay: 0xffff}
			alive := &groupMemberProxy{name: "alive", delay: 80}
			alive.alive.Store(true)
			g := build([]C.Proxy{dead, alive}, true)
			g.forceSet("dead")
			g.healthTest()
			assertPinKept(t, g, "dead")
		})
	}
}

func TestPinSurvivesHealthCheckWithoutReplacement(t *testing.T) {
	for kind, build := range pinnedGroupKinds() {
		t.Run(kind, func(t *testing.T) {
			dead := &groupMemberProxy{name: "dead", delay: 0xffff}
			g := build([]C.Proxy{dead}, false)
			g.forceSet("dead")
			g.healthTest()
			assertPinKept(t, g, "dead")
		})
	}
}

func TestPinSurvivesLostHealthCheckGuardRace(t *testing.T) {
	dead := &groupMemberProxy{name: "dead", delay: 0xffff}
	alive := &groupMemberProxy{name: "alive", delay: 80}
	alive.alive.Store(true)
	members := []C.Proxy{dead, alive}

	f := newTestFallback(1, members...)
	f.providers = []P.ProxyProvider{&readyTestProvider{members: members, ready: true}}
	fallbackPersist := &recordingManualSelectionPersistence{}
	f.selectionPersistence = fallbackPersist
	f.ForceSet("dead")
	f.failedTesting.Store(true)
	f.healthCheckForProxy(dead, f.selection.snapshot())
	assertPinKept(t, &pinnedGroup{name: "fallback", isManual: f.NowIsManual, now: f.Now, persist: fallbackPersist}, "dead")

	u := newFailedTimesURLTest(1, members...)
	u.providers = []P.ProxyProvider{&readyTestProvider{members: members, ready: true}}
	urlPersist := &recordingManualSelectionPersistence{}
	u.selectionPersistence = urlPersist
	u.ForceSet("dead")
	u.failedTesting.Store(true)
	u.healthCheckForSelection(u.selection.snapshot())
	assertPinKept(t, &pinnedGroup{name: "url-test", isManual: u.NowIsManual, now: u.Now, persist: urlPersist}, "dead")
}

// The pinned node vanishes from the group (provider refresh), then comes back.
func TestPinSurvivesNodeBeingTemporarilyMissing(t *testing.T) {
	pinned := &groupMemberProxy{name: "pinned", delay: 50}
	pinned.alive.Store(true)
	other := &groupMemberProxy{name: "other", delay: 80}
	other.alive.Store(true)

	f := newTestFallback(1, other)
	persist := &recordingManualSelectionPersistence{}
	f.selectionPersistence = persist
	f.ForceSet("pinned")
	if got := f.Now(); got != "other" {
		t.Fatalf("fallback Now() = %q while the pinned node is missing", got)
	}
	f.providerProxies = []C.Proxy{pinned, other}
	assertPinKept(t, &pinnedGroup{name: "fallback", isManual: f.NowIsManual, now: f.Now, persist: persist}, "pinned")

	u := newFailedTimesURLTest(1, other)
	urlPersist := &recordingManualSelectionPersistence{}
	u.selectionPersistence = urlPersist
	u.ForceSet("pinned")
	if got := u.Now(); got != "other" {
		t.Fatalf("url-test Now() = %q while the pinned node is missing", got)
	}
	u.providerProxies = []C.Proxy{pinned, other}
	u.fastSingle.Reset()
	assertPinKept(t, &pinnedGroup{name: "url-test", isManual: u.NowIsManual, now: u.Now, persist: urlPersist}, "pinned")
}

// The only way out is the user's explicit unpin.
func TestExplicitClearStillReleasesPin(t *testing.T) {
	node := &groupMemberProxy{name: "node", delay: 50}
	node.alive.Store(true)
	f := newTestFallback(1, node)
	f.ForceSet("node")
	f.ClearManualSelection()
	if f.NowIsManual() {
		t.Fatal("fallback: explicit clear did not release the pin")
	}
	u := newFailedTimesURLTest(1, node)
	u.ForceSet("node")
	u.ClearManualSelection()
	if u.NowIsManual() {
		t.Fatal("url-test: explicit clear did not release the pin")
	}
}
