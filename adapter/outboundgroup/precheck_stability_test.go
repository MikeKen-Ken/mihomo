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

type endpointPrecheckProxy struct {
	C.Proxy
	mu      sync.Mutex
	urls    []string
	allFail bool
}

func (p *endpointPrecheckProxy) Name() string { return "endpoint-proxy" }
func (p *endpointPrecheckProxy) URLTest(_ context.Context, url string, _ utils.IntRanges[uint16]) (uint16, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urls = append(p.urls, url)
	if url == "https://unavailable.example/probe" || p.allFail {
		return 0, errors.New("endpoint unavailable")
	}
	return 20, nil
}

func TestEndpointFailureDoesNotTriggerNodeFailover(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{Name: "probe-test", Type: C.Fallback, TestTimeout: 100})
	proxy := &endpointPrecheckProxy{}
	var disruptions atomic.Int32
	gb.scheduleCurrentProxyPreHealthCheck(proxy, "https://unavailable.example/probe", "204", "max-failed-times", false, proxyPrecheckCallbacks{
		onSuccess: func() { disruptions.Add(1) },
		onFailure: func() { disruptions.Add(1) },
	})
	waitForCondition(t, time.Second, func() bool { return !gb.connectTesting.Load() })
	if disruptions.Load() != 0 {
		t.Fatal("one unavailable endpoint triggered selection change/failover")
	}
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if len(proxy.urls) < 2 || proxy.urls[0] == proxy.urls[len(proxy.urls)-1] {
		t.Fatalf("no independent confirmation: %v", proxy.urls)
	}
}

func TestIndependentFailuresStillTriggerFailover(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{Name: "probe-test", Type: C.Fallback, TestTimeout: 100})
	var failures atomic.Int32
	gb.scheduleCurrentProxyPreHealthCheck(&endpointPrecheckProxy{allFail: true}, "https://unavailable.example/probe", "204", "max-failed-times", false, proxyPrecheckCallbacks{
		onFailure: func() { failures.Add(1) },
	})
	waitForCondition(t, time.Second, func() bool { return !gb.connectTesting.Load() })
	if failures.Load() != 1 {
		t.Fatalf("failovers = %d, want 1", failures.Load())
	}
}
