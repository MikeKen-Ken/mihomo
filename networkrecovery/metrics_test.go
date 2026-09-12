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
