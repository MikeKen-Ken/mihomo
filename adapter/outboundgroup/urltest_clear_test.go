package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/singledo"
	C "github.com/metacubex/mihomo/constant"
)

func TestURLTestClearManualSelectionResetsFastCache(t *testing.T) {
	u := &URLTest{
		GroupBase:  NewGroupBase(GroupBaseOption{Name: "Auto", Type: C.URLTest}),
		fastSingle: singledo.NewSingle[C.Proxy](time.Second * 10),
	}
	u.selectionPersistence = nil
	u.ForceSet("node-a")
	u.ClearManualSelection()
	if got := u.selection.snapshot().name; got != "" {
		t.Fatalf("selected should be empty, got %q", got)
	}
	if u.fastNode != nil {
		t.Fatal("fastNode should be cleared")
	}
}

func TestURLTestClearManualSelectionPersistsClear(t *testing.T) {
	u := &URLTest{
		GroupBase:  NewGroupBase(GroupBaseOption{Name: "Auto", Type: C.URLTest}),
		fastSingle: singledo.NewSingle[C.Proxy](time.Second * 10),
	}
	persistence := &recordingManualSelectionPersistence{}
	u.selectionPersistence = persistence
	u.ForceSet("node-a")
	u.ClearManualSelection()
	if got := persistence.cleared.Load(); got != 1 {
		t.Fatalf("persisted selection clears = %d, want 1", got)
	}
}

func TestURLTestHealthCheckPreservesPinWithoutVerifiedReplacement(t *testing.T) {
	u := &URLTest{
		GroupBase:  NewGroupBase(GroupBaseOption{Name: "Auto", Type: C.URLTest}),
		fastSingle: singledo.NewSingle[C.Proxy](time.Second * 10),
	}
	u.selectionPersistence = nil
	u.ForceSet("node-a")
	u.healthCheck()
	if got := u.selection.snapshot().name; got != "node-a" {
		t.Fatalf("selection changed without a verified replacement: %q", got)
	}
}
