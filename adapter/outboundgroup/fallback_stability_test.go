package outboundgroup

import (
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"testing"
	"time"
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
