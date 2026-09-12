package networkrecovery

import (
	"sync"
	"time"
)

const eventCapacity = 64

// Event measures an action, not restored application connectivity. Names are
// deliberately omitted so provider size and arbitrary configuration cannot grow it.
type Event struct {
	At         time.Time `json:"at"`
	Action     string    `json:"action"`
	DurationMS int64     `json:"durationMs"`
}

type Metrics struct {
	Events         []Event `json:"events"`
	TrafficVetoes  uint64  `json:"trafficVetoes"`
	FailedSearches uint64  `json:"failedSearches"`
	Switches       uint64  `json:"switches"`
}

var diagnostics struct {
	sync.Mutex
	events                   [eventCapacity]Event
	next, count              int
	vetoes, failed, switches uint64
}

// RecordEvent is only called on recovery decisions, never application reads.
func RecordEvent(action string, started time.Time) {
	switch action {
	case "traffic-veto", "backup-unavailable", "backup-verified", "switch", "dns-reset", "route-reset", "coalesced", "ignored":
	default:
		return
	}
	diagnostics.Lock()
	defer diagnostics.Unlock()
	diagnostics.events[diagnostics.next] = Event{time.Now(), action, time.Since(started).Milliseconds()}
	diagnostics.next = (diagnostics.next + 1) % eventCapacity
	if diagnostics.count < eventCapacity {
		diagnostics.count++
	}
	switch action {
	case "traffic-veto":
		diagnostics.vetoes++
	case "backup-unavailable":
		diagnostics.failed++
	case "switch":
		diagnostics.switches++
	}
}

func Diagnostics() Metrics {
	diagnostics.Lock()
	defer diagnostics.Unlock()
	result := Metrics{Events: make([]Event, diagnostics.count), TrafficVetoes: diagnostics.vetoes, FailedSearches: diagnostics.failed, Switches: diagnostics.switches}
	for i := range result.Events {
		result.Events[i] = diagnostics.events[(diagnostics.next-diagnostics.count+i+eventCapacity)%eventCapacity]
	}
	return result
}
