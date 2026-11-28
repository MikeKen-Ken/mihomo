package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// 选择组没有手动固定时用首次解析到的第一个成员。之后列表重排也不改这个默认，
// 取消手动选择会回到它。只有手动固定才会离开第一个成员。
func TestSelectorDefaultSurvivesMemberReorder(t *testing.T) {
	direct := &groupMemberProxy{name: "DIRECT"}
	auto := &groupMemberProxy{name: "Auto"}
	jp := &groupMemberProxy{name: "JP"}

	s, err := NewSelector(GroupCommonOption{Name: "Nodes"}, SelectorOption{}, &groupMemberProxy{name: "COMPATIBLE"}, []P.ProxyProvider{
		&orderedTestProvider{version: 1, members: []C.Proxy{direct, auto, jp}},
	})
	if err != nil {
		t.Fatalf("NewSelector: %v", err)
	}
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

	// An explicit pin wins and survives a reorder. Unpinning returns to the first member.
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
	if got := s.Now(); got != "DIRECT" {
		t.Fatalf("unpin = %q, want the first member DIRECT", got)
	}

	// The latched default vanished from the group: fall back to the first member.
	provider.members = []C.Proxy{auto}
	provider.version++
	if got := s.Now(); got != "Auto" {
		t.Fatalf("missing default did not fall back to first member: %q", got)
	}
}

func TestSelectorDefaultIsFirstMemberWithoutPin(t *testing.T) {
	s, err := NewSelector(GroupCommonOption{Name: "Nodes"}, SelectorOption{}, &groupMemberProxy{name: "COMPATIBLE"}, []P.ProxyProvider{
		&orderedTestProvider{
			version: 1,
			members: []C.Proxy{
				&groupMemberProxy{name: "DIRECT"},
				&groupMemberProxy{name: "Auto"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewSelector: %v", err)
	}
	if got := s.Now(); got != "DIRECT" {
		t.Fatalf("Now() = %q, want first member DIRECT", got)
	}
}

func TestOtherSelectorStillDefaultsToFirstMember(t *testing.T) {
	s, err := NewSelector(GroupCommonOption{Name: "Final"}, SelectorOption{}, &groupMemberProxy{name: "COMPATIBLE"}, []P.ProxyProvider{
		&orderedTestProvider{
			version: 1,
			members: []C.Proxy{
				&groupMemberProxy{name: "Auto"},
				&groupMemberProxy{name: "DIRECT"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewSelector: %v", err)
	}
	if got := s.Now(); got != "Auto" {
		t.Fatalf("Now() = %q, want first member Auto", got)
	}
}
