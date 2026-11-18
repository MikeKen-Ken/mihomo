package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// 选择组的默认节点是"组首次解析成员时的第一个成员"，不是每次取当前列表第一个。
// 否则 provider 刷新/联通评分重排会把 Direct 组从 DIRECT 悄悄切到 Auto。
func TestSelectorDefaultSurvivesMemberReorder(t *testing.T) {
	direct := &groupMemberProxy{name: "DIRECT"}
	auto := &groupMemberProxy{name: "Auto"}
	jp := &groupMemberProxy{name: "JP"}

	s := NewSelector(&GroupCommonOption{Name: "Direct"}, []P.ProxyProvider{
		&orderedTestProvider{version: 1, members: []C.Proxy{direct, auto, jp}},
	})
	provider := s.providers[0].(*orderedTestProvider)

	if got := s.Now(); got != "DIRECT" {
		t.Fatalf("initial Now() = %q, want DIRECT", got)
	}
	if s.NowIsManual() {
		t.Fatal("implicit default must not be reported as a manual pin")
	}

	// Reordering the members (provider refresh / connectivity order) must not
	// change the effective outbound of a group the user never pinned.
	provider.members = []C.Proxy{auto, jp, direct}
	provider.version++
	if got := s.Now(); got != "DIRECT" {
		t.Fatalf("Now() after reorder = %q, want DIRECT", got)
	}
	if got := s.Proxies()[0].Name(); got != "Auto" {
		t.Fatalf("displayed order = %q, want the new member order", got)
	}

	// An explicit pin wins and survives a reorder; unpinning keeps the node.
	if err := s.Set("JP"); err != nil {
		t.Fatalf("Set(JP): %v", err)
	}
	if !s.NowIsManual() || s.Now() != "JP" {
		t.Fatalf("explicit pin lost: manual=%v now=%q", s.NowIsManual(), s.Now())
	}
	provider.members = []C.Proxy{direct, auto, jp}
	provider.version++
	if got := s.Now(); got != "JP" {
		t.Fatalf("pin did not survive reorder: %q", got)
	}
	s.ClearManualSelection()
	if s.NowIsManual() {
		t.Fatal("ClearManualSelection kept the manual flag")
	}
	if got := s.Now(); got != "JP" {
		t.Fatalf("unpin changed the node to %q, want JP kept", got)
	}

	// The latched default vanished from the group: fall back to the first member.
	provider.members = []C.Proxy{auto}
	provider.version++
	if got := s.Now(); got != "Auto" {
		t.Fatalf("missing default did not fall back to first member: %q", got)
	}
}

func TestSelectorDefaultIsFirstMemberWithoutPin(t *testing.T) {
	s := NewSelector(&GroupCommonOption{Name: "Direct"}, []P.ProxyProvider{
		&orderedTestProvider{
			version: 1,
			members: []C.Proxy{
				&groupMemberProxy{name: "DIRECT"},
				&groupMemberProxy{name: "Auto"},
			},
		},
	})
	if got := s.Now(); got != "DIRECT" {
		t.Fatalf("Now() = %q, want first member DIRECT", got)
	}
}
