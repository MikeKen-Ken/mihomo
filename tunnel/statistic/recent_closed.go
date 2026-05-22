package statistic

import (
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/common/atomic"
)

const (
	recentClosedMaxEntries = 512
	recentClosedMaxAge     = 24 * time.Hour
)

// RecentClosedItem is a REJECT (or REJECT-DROP / PASS) connection closed before the UI poll interval could observe it.
type RecentClosedItem struct {
	Info     TrackerInfo
	ClosedAt time.Time
}

type recentClosedBuffer struct {
	mu      sync.Mutex
	entries []RecentClosedItem
}

var recentClosed recentClosedBuffer

func chainHasRejectOutbound(chain C.Chain) bool {
	for _, name := range chain {
		switch name {
		case "REJECT", "REJECT-DROP", "PASS":
			return true
		}
	}
	return false
}

func snapshotTrackerInfo(info *TrackerInfo) TrackerInfo {
	if info == nil {
		return TrackerInfo{}
	}
	snap := TrackerInfo{
		UUID:          info.UUID,
		Metadata:      info.Metadata,
		Start:         info.Start,
		Chain:         info.Chain,
		ProviderChain: info.ProviderChain,
		Rule:          info.Rule,
		RulePayload:   info.RulePayload,
		RuleDetail:    info.RuleDetail,
	}
	snap.UploadTotal = atomic.NewInt64(info.UploadTotal.Load())
	snap.DownloadTotal = atomic.NewInt64(info.DownloadTotal.Load())
	return snap
}

func recordRecentClosedIfNeeded(info *TrackerInfo) {
	if info == nil || !chainHasRejectOutbound(info.Chain) {
		return
	}
	now := time.Now()
	item := RecentClosedItem{
		Info:     snapshotTrackerInfo(info),
		ClosedAt: now,
	}
	recentClosed.mu.Lock()
	defer recentClosed.mu.Unlock()
	recentClosed.pruneLocked(now)
	recentClosed.entries = append(recentClosed.entries, item)
	if len(recentClosed.entries) > recentClosedMaxEntries {
		recentClosed.entries = recentClosed.entries[len(recentClosed.entries)-recentClosedMaxEntries:]
	}
}

func (b *recentClosedBuffer) pruneLocked(now time.Time) {
	cutoff := now.Add(-recentClosedMaxAge)
	i := 0
	for _, e := range b.entries {
		if e.ClosedAt.After(cutoff) {
			b.entries[i] = e
			i++
		}
	}
	b.entries = b.entries[:i]
}

func cloneRecentClosed() []RecentClosedItem {
	recentClosed.mu.Lock()
	defer recentClosed.mu.Unlock()
	now := time.Now()
	recentClosed.pruneLocked(now)
	if len(recentClosed.entries) == 0 {
		return nil
	}
	out := make([]RecentClosedItem, len(recentClosed.entries))
	copy(out, recentClosed.entries)
	return out
}
