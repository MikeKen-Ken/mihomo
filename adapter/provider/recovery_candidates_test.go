package provider

import (
	"context"
	"errors"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"sync"
	"testing"
	"time"
)

type recoveryTestProxy struct {
	C.Proxy
	name    string
	block   bool
	fail    bool
	history []C.DelayHistory
	started chan struct{}
	done    chan struct{}
	wait    <-chan struct{}
	once    sync.Once
}

func (p *recoveryTestProxy) Name() string                                   { return p.name }
func (p *recoveryTestProxy) AliveForTestUrl(string) bool                    { return !p.fail }
func (p *recoveryTestProxy) DelayHistoryForTestUrl(string) []C.DelayHistory { return p.history }
func (p *recoveryTestProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	if p.wait != nil {
		select {
		case <-p.wait:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if p.started != nil {
		p.once.Do(func() { close(p.started) })
	}
	if p.done != nil {
		defer close(p.done)
	}
	if p.block {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if p.fail {
		return 0, errors.New("offline")
	}
	return 20, nil
}

func TestRecoveryReturnsWithoutWaitingForSlowBatchMember(t *testing.T) {
	slow := &recoveryTestProxy{name: "slow", block: true, done: make(chan struct{}), started: make(chan struct{})}
	good := &recoveryTestProxy{name: "good", wait: slow.started}
	hc := NewHealthCheck(nil, "test", "https://example.test", 5000, 0, false, nil)
	defer hc.close()
	start := time.Now()
	if !hc.recoverCandidates("https://example.test", nil, []C.Proxy{slow, good}) {
		t.Fatal("no backup found")
	}
	if time.Since(start) > time.Second {
		t.Fatal("waited for a slow candidate")
	}
	select {
	case <-slow.done:
	case <-time.After(time.Second):
		t.Fatal("redundant probe did not terminate")
	}
}

func TestRecoveryPrioritizesFreshSuccessfulBackup(t *testing.T) {
	SetHealthCheckWorkerLimit(1)
	defer SetHealthCheckWorkerLimit(30)
	old := &recoveryTestProxy{name: "old", block: true, started: make(chan struct{})}
	fresh := &recoveryTestProxy{name: "fresh", history: []C.DelayHistory{{Time: time.Now(), Delay: 20}}}
	hc := NewHealthCheck(nil, "test", "https://example.test", 5000, 0, false, nil)
	defer hc.close()
	if !hc.recoverCandidates("https://example.test", nil, []C.Proxy{old, fresh}) {
		t.Fatal("fresh backup not selected")
	}
	select {
	case <-old.started:
		t.Fatal("stale candidate ran before fresh backup")
	default:
	}
}
