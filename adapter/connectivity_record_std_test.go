//go:build !cmfa

package adapter

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func TestRecordProxyConnectivityTestUsesFixedFailurePenalty(t *testing.T) {
	home := t.TempDir()
	previous := C.Path.HomeDir()
	C.SetHomeDir(home)
	t.Cleanup(func() { C.SetHomeDir(previous) })

	now := time.Date(2026, 9, 26, 8, 0, 0, 0, time.Local)
	recordProxyConnectivityTestLocked("dead", 0, 1000, now)
	recordProxyConnectivityTestLocked("fast", 30, 5000, now)
	recordProxyConnectivityTest("skipped", -1, 5000)

	raw, err := os.ReadFile(C.Path.Resolve("proxy-connectivity-stats.json"))
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	var document connectivityStatsDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	day := now.Format("2006-01-02")
	dead := document.Data["dead"].Days[day]
	if dead.Failure != 1 || dead.Success != 0 || dead.DelaySum != connectivityStatsPenaltyMs {
		t.Fatalf("failure sample = %+v, want one failure costing %d", dead, connectivityStatsPenaltyMs)
	}
	fast := document.Data["fast"].Days[day]
	if fast.Success != 1 || fast.Failure != 0 || fast.DelaySum != 30 {
		t.Fatalf("success sample = %+v", fast)
	}
	if _, recorded := document.Data["skipped"]; recorded {
		t.Fatal("canceled sample was recorded")
	}
}
