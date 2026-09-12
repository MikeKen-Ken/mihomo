package outboundgroup

import (
	"context"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"sync/atomic"
	"testing"
	"time"
)

// A slow probe may retain yesterday's alive flag when canceled. It must not
// outrank the backup that actually succeeded in the current recovery.
type winningProvider struct {
	readyTestProvider
	winner *groupMemberProxy
}

func (p *winningProvider) HealthCheckURLUntilHealthy(string, utils.IntRanges[uint16], map[string]struct{}) bool {
	p.winner.checkedAt.Store(time.Now().UnixNano())
	return true
}

func TestRecoveryUsesVerifiedBackupInsteadOfStaleAliveFlag(t *testing.T) {
	dead := &groupMemberProxy{name: "dead", delay: 0xffff}
	stale := &groupMemberProxy{name: "stale", delay: 5}
	good := &groupMemberProxy{name: "verified", delay: 80}
	stale.alive.Store(true)
	good.alive.Store(true)
	members := []C.Proxy{dead, stale, good}
	provider := &winningProvider{readyTestProvider: readyTestProvider{members: members}, winner: good}
	f := newTestFallback(1, members...)
	f.providers = []P.ProxyProvider{provider}
	f.ForceSet("dead")
	f.healthCheckForProxy(dead, f.selection.snapshot())
	if f.Now() != "verified" {
		t.Fatalf("fallback picked %s", f.Now())
	}
	u := newFailedTimesURLTest(1, members...)
	u.providers = []P.ProxyProvider{provider}
	u.ForceSet("dead")
	u.healthCheckForSelection(u.selection.snapshot())
	if u.Now() != "verified" {
		t.Fatalf("url-test picked %s", u.Now())
	}
}

func TestCanceledApplicationRequestDoesNotTriggerPrecheck(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{Name: "canceled", Type: C.Fallback, MaxFailedTimes: 1})
	var called atomic.Int32
	gb.onDialFailed(context.Background(), C.Shadowsocks, context.Canceled, nil, "", "", func() { called.Add(1) })
	if gb.connectTesting.Load() || gb.failedTimes != 0 || called.Load() != 0 {
		t.Fatal("cancellation triggered node recovery")
	}
}
