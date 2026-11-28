package outboundgroup

import (
	"context"
	C "github.com/metacubex/mihomo/constant"
	"net"
	"testing"
	"time"
)

type replyPacketConn struct {
	C.PacketConn
	released bool
}

func (c *replyPacketConn) ReadFrom(b []byte) (int, net.Addr, error) { b[0] = 1; return 1, nil, nil }
func (c *replyPacketConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return []byte{1}, func() { c.released = true }, nil, nil
}

func TestBothUDPReadPathsProvideHealthEvidence(t *testing.T) {
	for _, zeroCopy := range []bool{false, true} {
		gb := NewGroupBase(GroupBaseOption{Name: "udp-test", Type: C.Fallback})
		underlying := &replyPacketConn{}
		pc := gb.observePacketTraffic(context.Background(), underlying, &groupMemberProxy{name: "udp-node"})
		start := time.Now()
		if zeroCopy {
			data, put, _, err := pc.WaitReadFrom()
			if err != nil || len(data) != 1 {
				t.Fatal("reply changed")
			}
			put()
			if !underlying.released {
				t.Fatal("packet release callback lost")
			}
		} else {
			pc.ReadFrom(make([]byte, 1))
		}
		if !gb.traffic.receivedSince("udp-node", start) {
			t.Fatal("UDP reply was not health evidence")
		}
	}
}

func TestDiagnosticPacketsDoNotCountAsApplicationTraffic(t *testing.T) {
	gb := NewGroupBase(GroupBaseOption{Name: "udp-test", Type: C.Fallback})
	pc := &replyPacketConn{}
	if gb.observePacketTraffic(C.WithConnectivityProbe(context.Background()), pc, &groupMemberProxy{name: "probe"}) != pc {
		t.Fatal("diagnostic packet connection was treated as application traffic")
	}
}
