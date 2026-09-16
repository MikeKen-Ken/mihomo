package outboundgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

type backgroundPacketFailureProxy struct{ *groupMemberProxy }

func (p *backgroundPacketFailureProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	return nil, p.dialErr
}

func TestBackgroundDialFailureKeepsReachableManualPin(t *testing.T) {
	for _, kind := range []string{"fallback", "url-test", "url-test-udp"} {
		t.Run(kind, func(t *testing.T) {
			preferred := &groupMemberProxy{name: "automatic", delay: 10, urlDelay: 10}
			pinned := &groupMemberProxy{name: "pinned", delay: 80, urlDelay: 80, dialErr: errors.New("destination dial timeout")}
			preferred.alive.Store(true)
			pinned.alive.Store(true)
			var group interface {
				C.ProxyAdapter
				ForceSet(string)
				Now() string
				NowIsManual() bool
			}
			var base *GroupBase
			if kind == "fallback" {
				f := newTestFallback(1, preferred, pinned)
				group, base = f, f.GroupBase
			} else {
				u := newFailedTimesURLTest(1, preferred, &backgroundPacketFailureProxy{pinned})
				group, base = u, u.GroupBase
			}
			persistence := &recordingManualSelectionPersistence{}
			base.selectionPersistence = persistence
			group.ForceSet("pinned")
			var err error
			if kind == "url-test-udp" {
				_, err = group.ListenPacketContext(context.Background(), nil)
			} else {
				_, err = group.DialContext(context.Background(), nil)
			}
			if err == nil {
				t.Fatal("expected the destination dial to fail")
			}
			waitForCondition(t, time.Second, func() bool { return !base.connectTesting.Load() })
			if pinned.urlCalls.Load() != 1 {
				t.Fatal("did not check the pinned node's reachability")
			}
			if !group.NowIsManual() || group.Now() != "pinned" || persistence.cleared.Load() != 0 {
				t.Fatalf("healthy pin lost: manual=%v current=%s persisted clears=%d", group.NowIsManual(), group.Now(), persistence.cleared.Load())
			}
		})
	}
}
