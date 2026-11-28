package outboundgroup

import (
	"sort"
	"testing"
)

func TestLessByNameOrder(t *testing.T) {
	index := map[string]int{"b": 0, "a": 1, "c": 2}
	names := []string{"a", "c", "b", "other"}
	sort.SliceStable(names, func(i, j int) bool {
		return lessByNameOrder(names[i], names[j], index)
	})
	want := []string{"b", "a", "c", "other"}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("got %v want %v", names, want)
		}
	}
}

func TestLessByNameOrderKeepsUnknownRelativeOrder(t *testing.T) {
	index := map[string]int{"keep": 0}
	names := []string{"z", "keep", "y"}
	sort.SliceStable(names, func(i, j int) bool {
		return lessByNameOrder(names[i], names[j], index)
	})
	if names[0] != "keep" || names[1] != "z" || names[2] != "y" {
		t.Fatalf("got %v", names)
	}
}
