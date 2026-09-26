//go:build !cmfa

package outboundgroup

import "testing"

func TestConclusivePrechecksAreRecorded(t *testing.T) {
	var samples []int
	previous := recordFailedTimesConnectivity
	recordFailedTimesConnectivity = func(_ string, delay int, _ int) {
		samples = append(samples, delay)
	}
	t.Cleanup(func() { recordFailedTimesConnectivity = previous })

	recordConclusivePrecheck("max-connect-times", "node", 20, 5000)
	recordConclusivePrecheck("max-connect-times", "", 20, 5000)
	recordConclusivePrecheck("other", "node", 20, 5000)
	recordConclusivePrecheck("max-failed-times", "node", 20, 5000)
	recordConclusivePrecheck("max-failed-times", "node", 0, 1000)

	if len(samples) != 3 || samples[0] != 20 || samples[1] != 20 || samples[2] != 0 {
		t.Fatalf("samples = %v, want both checks and one failure", samples)
	}
}
