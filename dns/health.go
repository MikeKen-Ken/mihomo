package dns

import (
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/networkrecovery"
)

// DNS 连续失败自愈监控。
//
// 背景：mihomo 使用 singleflight 合并相同问题的并发查询。当 DoH/DoQ/DoT 上游连接僵死
// （既不返回也不报错）时，首个 in-flight 请求永不结束，相同问题的后续请求全部阻塞，
// 旧行为最终表现为「所有 DNS 解析失败」，且只能通过重启进程清掉阻塞请求。
//
// 策略：短时间内出现连续失败达到阈值时，先重置 DNS；恢复后仍连续失败时，
// 通过 networkrecovery 升级为完整路由恢复。两级动作都带去重，避免恢复风暴。

const (
	// 触发自愈的连续失败次数阈值
	healFailureThreshold = 5
	// 失败计数的滑动窗口
	healFailureWindow = 5 * time.Second
	// 恢复后的静默期必须覆盖默认 DNS 查询超时，避免旧请求尚未退出时重复恢复
	healGracePeriod = 10 * time.Second
	// 在此时间内再次恢复时记录为持续故障，便于区分新的独立故障
	healRetryWindow = 60 * time.Second
)

type healthMonitor struct {
	mu           sync.Mutex
	failures     []time.Time // 滑动窗口内的失败时间点
	quietUntil   time.Time   // 静默期截止：在此之前不计失败
	lastHealAt   time.Time   // 上次恢复时间；任一成功结果都会结束该恢复周期
}

var dnsHealth = &healthMonitor{}

// recordResult 记录一次真实上游解析结果（缓存命中不调用此函数，因此计数只反映上游健康度）。
func (h *healthMonitor) recordResult(success bool) {
	now := time.Now()

	h.mu.Lock()
	defer h.mu.Unlock()

	if success {
		// 任一成功即清零，确保「连续失败」语义；只要 DNS 整体可用就不会误触发
		if len(h.failures) > 0 {
			h.failures = h.failures[:0]
		}
		h.lastHealAt = time.Time{}
		networkrecovery.MarkHealthy()
		return
	}

	// 软恢复后的静默期内不计失败，给连接重建留出时间
	if now.Before(h.quietUntil) {
		return
	}

	// 追加失败并按窗口裁剪过期项
	h.failures = append(h.failures, now)
	cutoff := now.Add(-healFailureWindow)
	idx := 0
	for ; idx < len(h.failures); idx++ {
		if h.failures[idx].After(cutoff) {
			break
		}
	}
	if idx > 0 {
		h.failures = h.failures[idx:]
	}

	if len(h.failures) < healFailureThreshold {
		return
	}

	// 达到阈值，触发恢复并清空窗口
	h.failures = h.failures[:0]
	h.trigger(now)
}

// trigger 必须在持有锁时调用。
func (h *healthMonitor) trigger(now time.Time) {
	if !h.lastHealAt.IsZero() && now.Sub(h.lastHealAt) < healRetryWindow {
		log.Warnln("[DNS] Resolution failures continued after automatic recovery; escalating network recovery")
	} else {
		log.Warnln("[DNS] Consecutive resolution failures detected; clearing cache and resetting upstream connections")
	}

	h.lastHealAt = now
	h.quietUntil = now.Add(healGracePeriod)
	networkrecovery.Recover(networkrecovery.Request{
		Kind:   networkrecovery.KindDNSFailure,
		Reason: "consecutive upstream DNS failures",
	})
}
