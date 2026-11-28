package outboundgroup

import (
	"context"
	C "github.com/metacubex/mihomo/constant"
	"net"
)

// UDP application replies are health evidence too, including the zero-copy path.
type trafficPacketConn struct {
	C.PacketConn
	onRead func()
}

func (c *trafficPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if n > 0 {
		c.onRead()
	}
	return n, addr, err
}

func (c *trafficPacketConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	data, put, addr, err := c.PacketConn.WaitReadFrom()
	if len(data) > 0 {
		c.onRead()
	}
	return data, put, addr, err
}

func (gb *GroupBase) observePacketTraffic(ctx context.Context, pc C.PacketConn, proxy C.Proxy) C.PacketConn {
	if pc == nil || proxy == nil || C.SuppressGroupOutboundFailureStats(ctx) {
		return pc
	}
	name := proxy.Name()
	return &trafficPacketConn{PacketConn: pc, onRead: func() { gb.traffic.record(name) }}
}
