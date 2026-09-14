//go:build !cmfa

package adapter

import (
	"sync"
	"time"
)

const desktopStatsBatchLimit = 64
const desktopStatsBatchWindow = 2 * time.Millisecond

type desktopStatsSample struct {
	name           string
	delay, timeout int
	success        bool
	at             time.Time
}

type desktopStatsRequest struct {
	sample desktopStatsSample
	done   chan struct{}
}

type desktopStatsBatchWriter struct {
	once     sync.Once
	requests chan desktopStatsRequest
}

var desktopStatsWriter desktopStatsBatchWriter

func (w *desktopStatsBatchWriter) record(sample desktopStatsSample) {
	w.once.Do(func() {
		// At most 64 queued and 64 in the current transaction. Producers
		// apply backpressure, and no completed URLTest has a deferred write.
		w.requests = make(chan desktopStatsRequest, desktopStatsBatchLimit)
		go runDesktopStatsBatches(w.requests, persistDesktopStatsBatch)
	})
	done := make(chan struct{})
	w.requests <- desktopStatsRequest{sample: sample, done: done}
	<-done
}

func runDesktopStatsBatches(requests <-chan desktopStatsRequest, persist func([]desktopStatsSample)) {
	for first := range requests {
		batch := []desktopStatsRequest{first}
		timer := time.NewTimer(desktopStatsBatchWindow)
	collect:
		for len(batch) < desktopStatsBatchLimit {
			select {
			case request, ok := <-requests:
				if !ok {
					break collect
				}
				batch = append(batch, request)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		samples := make([]desktopStatsSample, len(batch))
		for i, request := range batch {
			samples[i] = request.sample
		}
		persist(samples)
		for _, request := range batch {
			close(request.done)
		}
	}
}
