package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/singledo"
	C "github.com/metacubex/mihomo/constant"
)

func TestURLTestClearManualSelectionResetsFastCache(t *testing.T) {
	u := &URLTest{
		selected:   "node-a",
		fastSingle: singledo.NewSingle[C.Proxy](time.Second * 10),
	}
	u.ClearManualSelection()
	if u.selected != "" {
		t.Fatalf("selected should be empty, got %q", u.selected)
	}
	if u.fastNode != nil {
		t.Fatal("fastNode should be cleared")
	}
}
