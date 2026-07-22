//go:build !cmfa

package adapter

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// 桌面端（非 cmfa）联通统计：与 Android connectivity.RecordDelayTestResult 同格式，
// 写入核心 home 下 proxy-connectivity-stats.json，供 verge 排序与 UI 读取。

const (
	desktopStatsRetentionDays     = 30
	desktopDefaultPenaltyDelayMs  = 5000
	desktopConnectivityStatsFile  = "proxy-connectivity-stats.json"
)

type desktopDayCounts struct {
	Success  int `json:"s"`
	Failure  int `json:"f"`
	DelaySum int `json:"ds,omitempty"`
}

type desktopProxyEntry struct {
	Days map[string]desktopDayCounts `json:"days"`
}

type desktopStatsFileV2 struct {
	V    int                           `json:"v"`
	Data map[string]desktopProxyEntry  `json:"data"`
}

var (
	desktopStatsMu    sync.Mutex
	desktopStatsCache map[string]desktopProxyEntry
)

func desktopStatsPath() string {
	return C.Path.Resolve(desktopConnectivityStatsFile)
}

func desktopTodayKey(now time.Time) string {
	return now.Format("2006-01-02")
}

func desktopCutoffDayKey(now time.Time) string {
	return now.AddDate(0, 0, -(desktopStatsRetentionDays - 1)).Format("2006-01-02")
}

func desktopPruneDays(days map[string]desktopDayCounts, now time.Time) {
	if len(days) == 0 {
		return
	}
	cutoff := desktopCutoffDayKey(now)
	for key := range days {
		if key < cutoff {
			delete(days, key)
		}
	}
}

func desktopLoadStatsFromDisk() map[string]desktopProxyEntry {
	cache := make(map[string]desktopProxyEntry)
	raw, err := os.ReadFile(desktopStatsPath())
	if err == nil && len(raw) > 0 {
		var file desktopStatsFileV2
		if json.Unmarshal(raw, &file) == nil && file.V == 2 && file.Data != nil {
			return file.Data
		}
	}
	return cache
}

func desktopPersistStats() {
	if desktopStatsCache == nil {
		return
	}
	payload := desktopStatsFileV2{V: 2, Data: desktopStatsCache}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	path := desktopStatsPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

// recordDesktopConnectivityStats 由 Proxy.URLTest 回调：成功记真实 delay，失败记 timeout 惩罚。
func recordDesktopConnectivityStats(proxyName string, delay int, timeoutMs int) {
	if proxyName == "" || proxyName == "DIRECT" || proxyName == "REJECT" {
		return
	}
	if delay == -2 || delay == -1 {
		return
	}

	effectiveTimeout := timeoutMs
	if effectiveTimeout <= 0 {
		effectiveTimeout = desktopDefaultPenaltyDelayMs
	}
	isSuccess := delay > 0 && delay <= effectiveTimeout

	now := time.Now()
	day := desktopTodayKey(now)

	desktopStatsMu.Lock()
	defer desktopStatsMu.Unlock()

	withConnectivityStatsDiskLock(func() {
		// 持盘锁后从磁盘重载，避免与 UI 清空/写入互相覆盖
		desktopStatsCache = desktopLoadStatsFromDisk()

		entry := desktopStatsCache[proxyName]
		if entry.Days == nil {
			entry.Days = make(map[string]desktopDayCounts)
		}
		counts := entry.Days[day]
		if isSuccess {
			counts.Success++
			counts.DelaySum += delay
		} else {
			counts.Failure++
			counts.DelaySum += effectiveTimeout
		}
		entry.Days[day] = counts
		desktopPruneDays(entry.Days, now)
		desktopStatsCache[proxyName] = entry
		desktopPersistStats()
	})
}
