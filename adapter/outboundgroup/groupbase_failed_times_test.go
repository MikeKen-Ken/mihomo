//go:build !cmfa

package outboundgroup

import "testing"

func TestOnlyConclusiveMaxFailedTimesIsRecorded(t *testing.T) {
	var samples []int
	previous := recordFailedTimesConnectivity
	recordFailedTimesConnectivity = func(_ string, delay int, _ int) {
		samples = append(samples, delay)
	}
	t.Cleanup(func() { recordFailedTimesConnectivity = previous })

	recordConclusiveMaxFailedTimes("max-connect-times", "node", 20, 5000)
	recordConclusiveMaxFailedTimes("max-failed-times", "", 20, 5000)
	recordConclusiveMaxFailedTimes("max-failed-times", "node", 20, 5000)
	recordConclusiveMaxFailedTimes("max-failed-times", "node", 0, 1000)

	if len(samples) != 2 || samples[0] != 20 || samples[1] != 0 {
		t.Fatalf("samples = %v, want one success and one failure", samples)
	}
}
