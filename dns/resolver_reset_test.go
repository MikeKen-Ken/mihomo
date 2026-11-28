package dns

import (
	"sync"
	"testing"
	"time"

	D "github.com/miekg/dns"
)

func TestResolverResetConnectionForgetsInFlightQuery(t *testing.T) {
	resolver := &Resolver{}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	first := resolver.group.DoChan("example.test.", func() (*D.Msg, error) {
		close(started)
		<-release
		return &D.Msg{}, nil
	})
	<-started

	resolver.ResetConnection()

	want := &D.Msg{}
	second := resolver.group.DoChan("example.test.", func() (*D.Msg, error) {
		return want, nil
	})
	select {
	case result := <-second:
		if result.Err != nil {
			t.Fatalf("new query failed after reset: %v", result.Err)
		}
		if result.Val != want {
			t.Fatal("new query joined the stale in-flight call")
		}
		if result.Shared {
			t.Fatal("new query was incorrectly marked as shared")
		}
	case <-time.After(time.Second):
		t.Fatal("new query remained blocked after reset")
	}

	unblock()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("original query did not finish after release")
	}
}
