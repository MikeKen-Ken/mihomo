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
	desktopStatsRetentionDays    = 30
	desktopDefaultPenaltyDelayMs = 5000
	desktopConnectivityStatsFile = "proxy-connectivity-stats.json"
)

type desktopDayCounts struct {
	Success  int `json:"s"`
	Failure  int `json:"f"`
	DelaySum int `json:"ds,omitempty"`
}

type desktopProxyEntry struct {
	Days          map[string]desktopDayCounts `json:"days"`
	LastSuccessAt int64                       `json:"ls,omitempty"`
}

type desktopStatsFileV2 struct {
	V    int                          `json:"v"`
	Data map[string]desktopProxyEntry `json:"data"`
	Sync json.RawMessage              `json:"_sync,omitempty"`
}

var (
	desktopStatsMu    sync.Mutex
	desktopStatsCache map[string]desktopProxyEntry
	desktopStatsSync  json.RawMessage
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

// desktopPruneExpiredEntries 清理过期天键；days 已空则删除 proxy 条目。
// 调用方须已持有 desktopStatsMu。
func desktopPruneExpiredEntries(now time.Time) {
	if desktopStatsCache == nil {
		return
	}
	for name, entry := range desktopStatsCache {
		if entry.Days == nil {
			delete(desktopStatsCache, name)
			continue
		}
		desktopPruneDays(entry.Days, now)
		if len(entry.Days) == 0 {
			delete(desktopStatsCache, name)
			continue
		}
		desktopStatsCache[name] = entry
	}
}

func desktopLoadStatsFromDisk() desktopStatsFileV2 {
	empty := desktopStatsFileV2{
		V:    2,
		Data: make(map[string]desktopProxyEntry),
	}
	raw, err := os.ReadFile(desktopStatsPath())
	if err == nil && len(raw) > 0 {
		var file desktopStatsFileV2
		if json.Unmarshal(raw, &file) == nil && file.V == 2 && file.Data != nil {
			return file
		}
	}
	return empty
}

func desktopPersistStats() {
	if desktopStatsCache == nil {
		return
	}
	payload := desktopStatsFileV2{
		V:    2,
		Data: desktopStatsCache,
		Sync: desktopStatsSync,
	}
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
// 成功与失败对称计数：只节流失败会高估不稳定节点，且与 Android 共享的计数口径不一致。
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
	isSuccess := delay > 0 && delay < effectiveTimeout

	desktopStatsWriter.record(desktopStatsSample{
		name: proxyName, delay: delay, timeout: effectiveTimeout,
		success: isSuccess, at: time.Now(),
	})
}

// Read the latest UI reset/sync state under the cross-process lock. Callers
// are acknowledged only after the entire batch has finished writing.
func persistDesktopStatsBatch(samples []desktopStatsSample) {
	desktopStatsMu.Lock()
	defer desktopStatsMu.Unlock()
	withConnectivityStatsDiskLock(func() {
		file := desktopLoadStatsFromDisk()
		desktopStatsCache = file.Data
		desktopStatsSync = file.Sync
		if len(samples) == 0 {
			return
		}
		for _, sample := range samples {
			applyDesktopStatsSample(sample)
		}
		desktopPruneExpiredEntries(time.Now())
		desktopPersistStats()
	})
}

func applyDesktopStatsSample(sample desktopStatsSample) {
	proxyName, now := sample.name, sample.at
	entry := desktopStatsCache[proxyName]
	if entry.Days == nil {
		entry.Days = make(map[string]desktopDayCounts)
	}
	day := desktopTodayKey(now)
	counts := entry.Days[day]
	if sample.success {
		counts.Success++
		counts.DelaySum += sample.delay
		if now.Unix() > entry.LastSuccessAt {
			entry.LastSuccessAt = now.Unix()
		}
	} else {
		counts.Failure++
		counts.DelaySum += sample.timeout
	}
	entry.Days[day] = counts
	desktopStatsCache[proxyName] = entry
}
