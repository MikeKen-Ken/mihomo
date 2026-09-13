package trie_test

import (
	"testing"

	"github.com/metacubex/mihomo/component/trie"
)

func TestDomainSetWildcardShadow(t *testing.T) {
	tests := []struct {
		domains         []string
		match, notMatch string
	}{
		{[]string{"*.example.com", "dead.a.example.com"}, "a.example.com", "b.a.example.com"},
		{[]string{"*.*.example.com", "dead.*.a.example.com", "dead.b.a.example.com"}, "b.a.example.com", "a.example.com"},
		{[]string{"*.*.*.example.com", "*.a.example.com"}, "b.c.a.example.com", "d.b.c.a.example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.match, func(t *testing.T) {
			tree := trie.New[struct{}]()
			for _, domain := range tc.domains {
				if err := tree.Insert(domain, struct{}{}); err != nil {
					t.Fatal(err)
				}
			}
			set := tree.NewDomainSet()
			if !set.Has(tc.match) || set.Has(tc.notMatch) {
				t.Fatal("incorrect wildcard overlap matching")
			}
			if matched, pattern := set.HasWithMatch(tc.match); !matched || pattern == "" {
				t.Fatalf("matched-rule reporting lost: %v %q", matched, pattern)
			}
			testDump(t, tree, set)
		})
	}
}
