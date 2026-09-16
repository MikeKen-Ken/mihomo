package outboundgroup

import (
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"testing"
)

func TestRuntimeOrderUsesFirstHealthySharedMember(t *testing.T) {
	for _, kind := range []string{"fallback", "url-test"} {
		t.Run(kind, func(t *testing.T) {
			a := &groupMemberProxy{name: "first", delay: 80}
			b := &groupMemberProxy{name: "second", delay: 10}
			b.alive.Store(true)
			var groups []interface {
				Now() string
				NowIsManual() bool
				ForceSet(string)
				ReorderCachedProxies([]string)
			}
			for range 2 {
				if kind == "fallback" {
					groups = append(groups, newTestFallback(1, a, b))
				} else {
					groups = append(groups, newFailedTimesURLTest(1, a, b))
				}
			}
			// One group observes the backup while the first shared test is pending.
			if groups[0].Now() != "second" {
				t.Fatal("expected initial backup")
			}
			a.alive.Store(true)
			for _, g := range groups {
				g.ReorderCachedProxies([]string{"first", "second"})
				if got := g.Now(); got != "first" {
					t.Fatalf("shared results selected %s instead of first", got)
				}
				if g.NowIsManual() {
					t.Fatal("ordering created a pin")
				}
				// A later health result must take effect without reordering again.
				a.alive.Store(false)
				if g.Now() != "second" {
					t.Fatal("failed preferred member did not fail over")
				}
				a.alive.Store(true)
				if g.Now() != "first" {
					t.Fatal("cached backup masked shared recovery")
				}
				g.ReorderCachedProxies([]string{"second", "first"})
				if g.Now() != "second" {
					t.Fatal("new order did not take effect")
				}
				g.ReorderCachedProxies([]string{"first", "second"})
				g.ForceSet("second")
				if g.Now() != "second" {
					t.Fatal("lost explicit manual choice")
				}
			}
		})
	}
}

type orderedTestProvider struct {
	P.ProxyProvider
	version uint32
	members []C.Proxy
}

func (p *orderedTestProvider) Version() uint32    { return p.version }
func (p *orderedTestProvider) Proxies() []C.Proxy { return p.members }

func TestRuntimeOrderSurvivesProviderRefresh(t *testing.T) {
	a := &groupMemberProxy{name: "first", delay: 80}
	b := &groupMemberProxy{name: "second", delay: 10}
	a.alive.Store(true)
	b.alive.Store(true)
	f := newTestFallback(1, a, b)
	provider := &orderedTestProvider{version: 1, members: []C.Proxy{b, a}}
	f.providers = []P.ProxyProvider{provider}
	f.ReorderCachedProxies([]string{"first", "second"})
	provider.version++
	if got := f.Now(); got != "first" {
		t.Fatalf("provider refresh lost runtime order: %s", got)
	}
	if f.Proxies()[0].Name() != "first" {
		t.Fatal("displayed order disagrees with automatic choice")
	}
}
