package outboundgroup

import (
	"net/url"
	"strings"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/networkrecovery"
)

// Keep only the latest observed proxy: this is bounded regardless of provider size.
type trafficEvidence struct {
	mu             sync.Mutex
	proxy          string
	at             time.Time
	coreReportedAt time.Time
}

func recordProbeHealth(proxy C.Proxy, url string, delay uint16, healthy bool) {
	if recorder, ok := proxy.(interface{ RecordProbeHealth(string, uint16, bool) }); ok {
		recorder.RecordProbeHealth(url, delay, healthy)
	}
}

func (e *trafficEvidence) record(proxy string) {
	e.mu.Lock()
	now := time.Now()
	markCore := now.Sub(e.coreReportedAt) >= time.Second
	if markCore {
		e.coreReportedAt = now
	}
	e.proxy, e.at = proxy, now
	e.mu.Unlock()
	if markCore {
		networkrecovery.MarkTrafficHealthy()
	}
}

func (e *trafficEvidence) receivedSince(proxy string, since time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.proxy == proxy && !e.at.Before(since)
}

// Confirmation deliberately uses a different operator from the group endpoint.
// Both requests travel through the candidate proxy, never through DIRECT.
func independentProbeURL(primary string) string {
	parsed, err := url.Parse(primary)
	if err == nil && (strings.HasSuffix(strings.ToLower(parsed.Hostname()), ".cloudflare.com") || strings.EqualFold(parsed.Hostname(), "cloudflare.com")) {
		return "https://www.gstatic.com/generate_204"
	}
	return "https://cp.cloudflare.com/generate_204"
}

func (gb *GroupBase) verifiedRecoveryCandidate(url string, since time.Time) C.Proxy {
	for _, proxy := range gb.GetProxies(false) {
		if !proxy.AliveForTestUrl(url) {
			continue
		}
		if source, ok := proxy.(interface{ DelayHistoryForTestUrl(string) []C.DelayHistory }); ok {
			history := source.DelayHistoryForTestUrl(url)
			if len(history) > 0 {
				last := history[len(history)-1]
				if last.Delay > 0 && !last.Time.Before(since) && int(last.Delay) <= gb.TestTimeout {
					return proxy
				}
			}
		}
	}
	return nil
}
