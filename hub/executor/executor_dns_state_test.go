package executor

import (
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/dns"
)

func TestMakeDNSStateKey(t *testing.T) {
	cfg := &config.DNS{
		Enable:         true,
		EnhancedMode:   C.DNSFakeIP,
		FakeIPRange:    netip.MustParsePrefix("198.18.0.1/16"),
		FakeIPRange6:   netip.MustParsePrefix("fdfe:dcba:9876::1/64"),
		FakeIPTTL:      1,
		NameServer: []dns.NameServer{{
			Net:    "https",
			Addr:   "1.1.1.1",
			Params: map[string]string{"b": "2", "a": "1"},
		}},
		DefaultNameserver: []dns.NameServer{
			{Net: "udp", Addr: "1.1.1.1:53"},
		},
	}

	before := makeDNSStateKey(cfg, true)
	if after := makeDNSStateKey(cfg, true); after != before {
		t.Fatalf("unchanged DNS config produced different state keys: %q != %q", before, after)
	}

	cfg.FakeIPRange = netip.MustParsePrefix("198.19.0.1/16")
	if after := makeDNSStateKey(cfg, true); after == before {
		t.Fatal("changed fake-ip range did not change DNS state key")
	}

	if nameServerStateKey([]dns.NameServer{{Params: map[string]string{"b": "2", "a": "1"}}}) !=
		nameServerStateKey([]dns.NameServer{{Params: map[string]string{"a": "1", "b": "2"}}}) {
		t.Fatal("DNS state key changed with only parameter map iteration order")
	}
}
