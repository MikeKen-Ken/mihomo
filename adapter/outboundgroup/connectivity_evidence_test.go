package outboundgroup

import (
	"testing"
	"time"
)

func TestCheckedProxyEvidenceSurvivesOtherConnections(t *testing.T) {
	var e trafficEvidence
	e.beginCheck("A")
	start := time.Now()
	e.record("A")
	for i := 0; i < 1000; i++ {
		e.record("B")
	}
	if !e.receivedSince("A", start) {
		t.Fatal("old B connection erased A's reply")
	}
	e.endCheck()
	e.beginCheck("A")
	if e.receivedSince("A", time.Now()) {
		t.Fatal("previous check leaked into new check")
	}
	e.endCheck()
}

func TestOtherProxyCannotVetoFailure(t *testing.T) {
	var e trafficEvidence
	e.beginCheck("A")
	start := time.Now()
	e.record("B")
	if e.receivedSince("A", start) {
		t.Fatal("unrelated traffic vetoed failure")
	}
	e.endCheck()
}
