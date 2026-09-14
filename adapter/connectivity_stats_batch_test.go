//go:build !cmfa

package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func TestDesktopStatsBatchBoundAndAcknowledgement(t *testing.T) {
	requests := make(chan desktopStatsRequest, 130)
	done := make([]chan struct{}, 130)
	for i := range done {
		done[i] = make(chan struct{})
		requests <- desktopStatsRequest{sample: desktopStatsSample{name: fmt.Sprint(i)}, done: done[i]}
	}
	close(requests)
	total, batches := 0, 0
	runDesktopStatsBatches(requests, func(samples []desktopStatsSample) {
		if len(samples) > desktopStatsBatchLimit {
			t.Fatal("unbounded batch")
		}
		for i := total; i < total+len(samples); i++ {
			select {
			case <-done[i]:
				t.Fatal("acknowledged before persistence")
			default:
			}
		}
		total += len(samples)
		batches++
	})
	if total != 130 || batches != 3 {
		t.Fatalf("total=%d batches=%d", total, batches)
	}
	for _, ch := range done {
		select {
		case <-ch:
		default:
			t.Fatal("missing acknowledgement")
		}
	}
}

func statsTestHome(t testing.TB) {
	old := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	desktopLastFailureAt = nil
	t.Cleanup(func() {
		C.SetHomeDir(old)
		desktopStatsCache = nil
		desktopStatsSync = nil
		desktopLastFailureAt = nil
	})
}

func TestDesktopStatsRecordVisibleOnReturn(t *testing.T) {
	statsTestHome(t)
	recordDesktopConnectivityStats("visible", 40, 5000)
	file := desktopLoadStatsFromDisk()
	entry := file.Data["visible"]
	if entry.Days[desktopTodayKey(time.Now())].Success != 1 {
		t.Fatal("URLTest callback returned before its result was persisted")
	}
}

func TestDesktopStatsBatchPreservesResetAndSync(t *testing.T) {
	statsTestHome(t)
	now := time.Now()
	failure := desktopStatsSample{name: "node", timeout: 5000, at: now}
	persistDesktopStatsBatch([]desktopStatsSample{failure, failure})
	if got := desktopLoadStatsFromDisk().Data["node"].Days[desktopTodayKey(now)].Failure; got != 1 {
		t.Fatalf("duplicate failures: %d", got)
	}
	// Simulate a UI reset/sync transaction between completed batches.
	raw := []byte(`{"v":2,"data":{"remote":{"days":{"` + desktopTodayKey(now) + `":{"s":7,"f":0}}}},"_sync":{"resetWatermarks":{"node":{"counter":2,"deviceId":"desktop"}}}}`)
	if err := os.WriteFile(desktopStatsPath(), raw, 0600); err != nil {
		t.Fatal(err)
	}
	success := desktopStatsSample{name: "node", success: true, delay: 25, at: now}
	persistDesktopStatsBatch([]desktopStatsSample{failure, success})
	file := desktopLoadStatsFromDisk()
	got := file.Data["node"].Days[desktopTodayKey(now)]
	if got.Success != 1 || got.Failure != 1 || got.DelaySum != 5025 {
		t.Fatalf("counts: %+v", got)
	}
	if file.Data["remote"].Days[desktopTodayKey(now)].Success != 7 {
		t.Fatal("lost remote counts")
	}
	var before desktopStatsFileV2
	if err := json.Unmarshal(raw, &before); err != nil {
		t.Fatal(err)
	}
	if string(file.Sync) != string(before.Sync) {
		t.Fatal("changed sync metadata")
	}
}

func BenchmarkDesktopStatsPersistence(b *testing.B) {
	for _, batchSize := range []int{1, 64} {
		b.Run(fmt.Sprintf("batch-%d", batchSize), func(b *testing.B) {
			statsTestHome(b)
			now := time.Now()
			seed := make([]desktopStatsSample, 1000)
			for i := range seed {
				seed[i] = desktopStatsSample{name: fmt.Sprint(i), success: true, delay: 25, at: now}
			}
			persistDesktopStatsBatch(seed)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for start := 0; start < 64; start += batchSize {
					persistDesktopStatsBatch(seed[start : start+batchSize])
				}
			}
		})
	}
}
