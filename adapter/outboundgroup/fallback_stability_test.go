package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel"
)

type readyTestProvider struct {
	P.ProxyProvider
	members []C.Proxy
	ready   bool
}

func (p *readyTestProvider) Version() uint32    { return 1 }
func (p *readyTestProvider) Proxies() []C.Proxy { return p.members }
func (p *readyTestProvider) HealthCheckURLUntilHealthy(string, utils.IntRanges[uint16], map[string]struct{}) bool {
	if p.ready {
		for _, proxy := range p.members {
			if member, ok := proxy.(*groupMemberProxy); ok && member.alive.Load() {
				member.checkedAt.Store(time.Now().UnixNano())
			}
		}
	}
	return p.ready
}

func TestFailedBackupCheckKeepsSelection(t *testing.T) {
	dead := &groupMemberProxy{name: "dead", delay: 0xffff}
	f := newTestFallback(1, dead)
	f.providers = []P.ProxyProvider{&readyTestProvider{members: []C.Proxy{dead}}}
	f.ForceSet("dead")
	f.healthCheckForProxy(dead, f.selection.snapshot())
	if !f.NowIsManual() {
		t.Fatal("removed selection before any replacement passed")
	}
}

func TestConcurrentHealthCheckReleasesPin(t *testing.T) {
	dead := &groupMemberProxy{name: "dead", delay: 0xffff}
	alive := &groupMemberProxy{name: "alive", delay: 80}
	alive.alive.Store(true)
	f := newTestFallback(1, dead, alive)
	f.providers = []P.ProxyProvider{&readyTestProvider{members: []C.Proxy{dead, alive}, ready: true}}
	f.ForceSet("dead")
	f.failedTesting.Store(true)
	f.healthCheckForProxy(dead, f.selection.snapshot())
	if f.NowIsManual() {
		t.Fatal("pin survived a health check that never ran")
	}
}

type fallbackAdapterProxy struct {
	C.Proxy
	group *Fallback
}

func (p fallbackAdapterProxy) Adapter() C.ProxyAdapter { return p.group }

func TestConcurrentHealthCheckReleasesSiblingFallbackPin(t *testing.T) {
	oldProxies, oldProviders := tunnel.Proxies(), tunnel.Providers()
	t.Cleanup(func() { tunnel.UpdateProxies(oldProxies, oldProviders) })

	shared := &groupMemberProxy{name: "shared", delay: 0xffff}
	backup := &groupMemberProxy{name: "backup", delay: 80}
	backup.alive.Store(true)
	members := []C.Proxy{shared, backup}
	auto := newTestFallback(1, members...)
	auto.providers = []P.ProxyProvider{&readyTestProvider{members: members, ready: true}}
	nohk := NewFallback(&GroupCommonOption{Name: "NoHK", URL: testGroupURL, ExpectedStatus: "204", TestTimeout: 1000, MaxFailedTimes: 1}, nil)
	nohk.selectionPersistence = nil
	nohk.providerProxies = members
	nohk.providers = []P.ProxyProvider{&readyTestProvider{members: members, ready: true}}
	auto.ForceSet("shared")
	nohk.ForceSet("shared")
	nohk.failedTesting.Store(true)
	tunnel.UpdateProxies(map[string]C.Proxy{
		"Auto": fallbackAdapterProxy{group: auto},
		"NoHK": fallbackAdapterProxy{group: nohk},
	}, nil)

	auto.healthCheckForProxy(shared, auto.selection.snapshot())
	if auto.NowIsManual() {
		t.Fatal("tested fallback stayed pinned")
	}
	if nohk.NowIsManual() {
		t.Fatal("sibling fallback pin survived a skipped health check")
	}
}

func TestFallbackDoesNotImmediatelyReturnToFlappingPreferredNode(t *testing.T) {
	a := &groupMemberProxy{name: "preferred", delay: 40}
	b := &groupMemberProxy{name: "backup", delay: 80}
	a.alive.Store(true)
	b.alive.Store(true)
	f := newTestFallback(1, a, b)
	if f.Now() != a.name {
		t.Fatal("wrong initial priority")
	}
	a.alive.Store(false)
	if f.Now() != b.name {
		t.Fatal("did not fail over")
	}
	a.alive.Store(true)
	if f.Now() != b.name {
		t.Fatal("immediate failback to flapping node")
	}
	f.stable.mu.Lock()
	f.stable.until = time.Now().Add(-time.Second)
	f.stable.mu.Unlock()
	if f.Now() != a.name {
		t.Fatal("preferred priority not restored after hold period")
	}
	a.alive.Store(false)
	if f.Now() != b.name {
		t.Fatal("hold period blocked genuine failure")
	}
}
