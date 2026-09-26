//go:build !cmfa

package adapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

const (
	connectivityStatsPenaltyMs = 5000
	connectivityStatsRetention = 30
)

type connectivityStatsDocument struct {
	V    int                              `json:"v"`
	Data map[string]connectivityStatsNode `json:"data"`
	Sync json.RawMessage                  `json:"_sync,omitempty"`
}

type connectivityStatsNode struct {
	Days          map[string]connectivityStatsDay `json:"days"`
	LastSuccessAt int64                           `json:"ls,omitempty"`
}

type connectivityStatsDay struct {
	Success  int64 `json:"s"`
	Failure  int64 `json:"f"`
	DelaySum int64 `json:"ds,omitempty"`
}

var connectivityStatsMu sync.Mutex

func recordProxyConnectivityTest(proxyName string, delay int, timeoutMs int) {
	if proxyName == "" || proxyName == "DIRECT" || proxyName == "REJECT" || delay == -1 || delay == -2 {
		return
	}
	if timeoutMs <= 0 {
		timeoutMs = connectivityStatsPenaltyMs
	}
	connectivityStatsMu.Lock()
	defer connectivityStatsMu.Unlock()
	recordProxyConnectivityTestLocked(proxyName, delay, timeoutMs, time.Now())
}

func recordProxyConnectivityTestLocked(proxyName string, delay int, timeoutMs int, now time.Time) {
	path := C.Path.Resolve("proxy-connectivity-stats.json")
	document := connectivityStatsDocument{V: 2, Data: map[string]connectivityStatsNode{}}
	if raw, err := os.ReadFile(path); err == nil && len(raw) > 0 {
		if json.Unmarshal(raw, &document) != nil || document.V != 2 {
			return
		}
		if document.Data == nil {
			document.Data = map[string]connectivityStatsNode{}
		}
	}

	node := document.Data[proxyName]
	if node.Days == nil {
		node.Days = map[string]connectivityStatsDay{}
	}
	dayKey := now.Format("2006-01-02")
	counts := node.Days[dayKey]
	if delay > 0 && delay < timeoutMs {
		counts.Success++
		counts.DelaySum += int64(delay)
		node.LastSuccessAt = now.Unix()
	} else {
		counts.Failure++
		counts.DelaySum += connectivityStatsPenaltyMs
	}
	node.Days[dayKey] = counts
	pruneConnectivityStatsDays(node.Days, now)
	document.Data[proxyName] = node
	document.V = 2

	encoded, err := json.Marshal(document)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".proxy-connectivity-stats-*.tmp")
	if err != nil {
		return
	}
	temporaryPath := temporary.Name()
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
		return
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Remove(path)
		if err := os.Rename(temporaryPath, path); err != nil {
			_ = os.Remove(temporaryPath)
		}
	}
}

func pruneConnectivityStatsDays(days map[string]connectivityStatsDay, now time.Time) {
	cutoff := now.AddDate(0, 0, -(connectivityStatsRetention - 1)).Format("2006-01-02")
	for key := range days {
		if key < cutoff {
			delete(days, key)
		}
	}
}
