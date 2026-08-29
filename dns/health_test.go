package dns

import (
	"testing"
	"time"
)

func TestHealthMonitorSuccessEndsRecoveryCycle(t *testing.T) {
	now := time.Now()
	monitor := &healthMonitor{
		failures:   []time.Time{now},
		lastHealAt: now,
	}

	monitor.recordResult(true)

	if len(monitor.failures) != 0 {
		t.Fatalf("failures were not cleared: %d remain", len(monitor.failures))
	}
	if !monitor.lastHealAt.IsZero() {
		t.Fatal("successful DNS result did not end the recovery cycle")
	}
}
