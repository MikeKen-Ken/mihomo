package provider

import (
	"context"
	"sort"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

// Recovery uses a small rolling window. Return at the first verified success,
// cancel redundant probes, and never wait for a slow peer in the same batch.
func (hc *HealthCheck) recoverCandidates(url string, status utils.IntRanges[uint16], proxies []C.Proxy) bool {
	type candidate struct {
		proxy C.Proxy
		fresh bool
	}
	ordered := make([]candidate, 0, len(proxies))
	for _, proxy := range proxies {
		fresh := false
		if history, ok := proxy.(interface{ DelayHistoryForTestUrl(string) []C.DelayHistory }); ok {
			records := history.DelayHistoryForTestUrl(url)
			if len(records) > 0 {
				last := records[len(records)-1]
				fresh = last.Delay > 0 && time.Since(last.Time) < 5*time.Minute
			}
		}
		ordered = append(ordered, candidate{proxy, fresh})
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].fresh && !ordered[j].fresh })
	ctx, cancel := context.WithCancel(hc.ctx)
	defer cancel()
	limit := min(EffectiveHealthCheckWorkerLimit(), len(ordered))
	results := make(chan bool, limit)
	launch := func(proxy C.Proxy) {
		go func() {
			if !AcquireHealthCheckWorker(ctx) {
				results <- false
				return
			}
			defer ReleaseHealthCheckWorker()
			if ctx.Err() != nil {
				results <- false
				return
			}
			probe, done := context.WithTimeout(C.WithHealthCheckSourceName(ctx, hc.name), hc.timeout)
			defer done()
			_, err := proxy.URLTest(probe, url, status)
			results <- err == nil && probe.Err() == nil && proxy.AliveForTestUrl(url)
		}()
	}
	next, active := 0, 0
	for next < min(3, limit) {
		launch(ordered[next].proxy)
		next++
		active++
	}
	for active > 0 {
		select {
		case <-ctx.Done():
			return false
		case healthy := <-results:
			active--
			if healthy {
				return true
			}
			// If the small ready set fails, widen the search without exceeding
			// the configured global ceiling. Large dead lists must not serialize.
			for next < len(ordered) && active < limit {
				launch(ordered[next].proxy)
				next++
				active++
			}
		}
	}
	return false
}
