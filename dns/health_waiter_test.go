package dns

import (
	"context"
	"errors"
	D "github.com/miekg/dns"
	"sync"
	"testing"
	"time"
)

type pendingDNSClient struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *pendingDNSClient) Address() string  { return "test://pending" }
func (c *pendingDNSClient) ResetConnection() {}
func (c *pendingDNSClient) ExchangeContext(ctx context.Context, _ *D.Msg) (*D.Msg, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		return nil, errors.New("upstream failed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCanceledWaitersDoNotCountAsUpstreamDNSFailures(t *testing.T) {
	client := &pendingDNSClient{started: make(chan struct{}), release: make(chan struct{})}
	r := &Resolver{main: []dnsClient{client}}
	// Use the real shared monitor; no other test in this package runs in parallel.
	dnsHealth.recordResult(true)
	question := new(D.Msg)
	question.SetQuestion("waiter.example.", D.TypeTXT)
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r.exchangeWithoutCache(ctx, question)
	}
	<-client.started
	dnsHealth.mu.Lock()
	failures := len(dnsHealth.failures)
	dnsHealth.mu.Unlock()
	close(client.release)
	// Drain the actual flight before inspecting/updating global health again.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		dnsHealth.mu.Lock()
		count := len(dnsHealth.failures)
		dnsHealth.mu.Unlock()
		if count > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if failures != 0 {
		t.Fatalf("canceled waiters counted %d failures before upstream completed", failures)
	}
}
