package networkrecovery

import (
	"testing"
	"time"
)

func TestDiagnosticsBoundedAndDetached(t *testing.T) {
	before := Diagnostics()
	for i := 0; i < 1000; i++ {
		RecordEvent("traffic-veto", time.Now())
	}
	after := Diagnostics()
	if len(after.Events) != eventCapacity || after.TrafficVetoes != before.TrafficVetoes+1000 {
		t.Fatal("incorrect bound or counter")
	}
	after.Events[0].Action = "modified"
	if Diagnostics().Events[0].Action == "modified" {
		t.Fatal("snapshot aliases storage")
	}
	RecordEvent("arbitrary unbounded input", time.Now())
	if Diagnostics().Events[eventCapacity-1].Action != "traffic-veto" {
		t.Fatal("unknown action retained")
	}
}

func TestDiagnosticsTrafficEvidenceDoesNotClaimResetSuccess(t *testing.T) {
	previous := defaultManager.trafficSuccess.Swap(0)
	defer defaultManager.trafficSuccess.Store(previous)
	RecordEvent("route-reset", time.Now())
	if Diagnostics().LastTrafficAt != nil {
		t.Fatal("a reset is not application traffic evidence")
	}
	MarkTrafficHealthy()
	snapshot := Diagnostics()
	if snapshot.LastTrafficAt == nil || snapshot.LastTrafficAt.After(snapshot.ObservedAt) {
		t.Fatal("missing or invalid traffic observation")
	}
	*snapshot.LastTrafficAt = time.Time{}
	if Diagnostics().LastTrafficAt.IsZero() {
		t.Fatal("traffic snapshot aliases state")
	}
}
