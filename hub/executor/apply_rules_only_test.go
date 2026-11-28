package executor

import (
	"testing"

	"github.com/metacubex/mihomo/tunnel"
)

func TestApplyRulesOnlySwapsMatcherWithoutFullReload(t *testing.T) {
	cfg, err := ParseWithBytes([]byte("mode: rule\nrules:\n  - MATCH,DIRECT\n"))
	if err != nil {
		t.Fatalf("parse direct matcher: %v", err)
	}
	ApplyRulesOnly(cfg)

	rules := tunnel.Rules()
	if len(rules) != 1 {
		t.Fatalf("want 1 rule, got %d", len(rules))
	}
	if got := rules[0].Adapter(); got != "DIRECT" {
		t.Fatalf("want DIRECT, got %s", got)
	}

	cfg, err = ParseWithBytes([]byte("mode: rule\nrules:\n  - MATCH,REJECT\n"))
	if err != nil {
		t.Fatalf("parse reject matcher: %v", err)
	}
	ApplyRulesOnly(cfg)

	rules = tunnel.Rules()
	if len(rules) != 1 {
		t.Fatalf("want 1 rule after swap, got %d", len(rules))
	}
	if got := rules[0].Adapter(); got != "REJECT" {
		t.Fatalf("want REJECT, got %s", got)
	}
}
