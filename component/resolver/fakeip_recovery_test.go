package resolver

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeIPMapperStub struct {
	deleted []netip.Addr
}

func (f *fakeIPMapperStub) FakeIPEnabled() bool              { return true }
func (f *fakeIPMapperStub) MappingEnabled() bool             { return true }
func (f *fakeIPMapperStub) IsFakeIP(ip netip.Addr) bool      { return ip == netip.MustParseAddr("198.18.0.98") }
func (f *fakeIPMapperStub) IsFakeBroadcastIP(netip.Addr) bool { return false }
func (f *fakeIPMapperStub) IsExistFakeIP(ip netip.Addr) bool   { return f.IsFakeIP(ip) }
func (f *fakeIPMapperStub) FindHostByIP(netip.Addr) (string, bool) {
	return "", false
}
func (f *fakeIPMapperStub) FlushFakeIP() error { return nil }
func (f *fakeIPMapperStub) DeleteFakeIPMapping(ip netip.Addr) {
	f.deleted = append(f.deleted, ip)
}
func (f *fakeIPMapperStub) InsertHostByIP(netip.Addr, string) {}
func (f *fakeIPMapperStub) StoreFakePoolState()               {}

func TestFakeIPRecoveryRepairsSingleIPOnly(t *testing.T) {
	ip := netip.MustParseAddr("198.18.0.98")
	stub := &fakeIPMapperStub{}

	oldMapper := DefaultHostMapper
	DefaultHostMapper = stub
	defer func() { DefaultHostMapper = oldMapper }()

	tracker := fakeIPRecovery{
		byIP:     make(map[netip.Addr]fakeIPMissState),
		lastWarn: make(map[netip.Addr]time.Time),
	}

	for i := 0; i < fakeIPMissThreshold; i++ {
		tracker.onMiss(ip)
	}

	assert.Len(t, stub.deleted, 1)
	assert.Equal(t, ip, stub.deleted[0])

	other := netip.MustParseAddr("198.18.0.21")
	stub.deleted = nil
	for i := 0; i < fakeIPMissThreshold-1; i++ {
		tracker.onMiss(other)
	}
	assert.Empty(t, stub.deleted)
}
