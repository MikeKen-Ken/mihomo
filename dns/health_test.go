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

func TestOneFailingDomainCannotTriggerNetworkRecovery(t *testing.T) {
	m := &healthMonitor{}
	for i := 0; i < 100; i++ {
		m.recordExchange("blocked.example.", false)
	}
	if !m.lastHealAt.IsZero() {
		t.Fatal("one failing destination triggered DNS recovery")
	}
	if len(m.failures) > healFailureThreshold || len(m.domains) > healFailureThreshold {
		t.Fatal("unbounded failure evidence")
	}
}

func TestIndependentDNSFailuresTriggerSoftRecovery(t *testing.T) {
	m := &healthMonitor{}
	for _, name := range []string{"a.example", "b.example", "c.example", "a.example", "b.example"} {
		m.recordExchange(name, false)
	}
	if m.lastHealAt.IsZero() {
		t.Fatal("broad DNS failure did not recover")
	}
}
