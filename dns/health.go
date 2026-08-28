package dns

import (
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/log"
)

// DNS 连续失败自愈监控。
//
// 背景：mihomo 使用 singleflight 合并相同问题的并发查询。当 DoH/DoQ/DoT 上游连接僵死
// （既不返回也不报错）时，首个 in-flight 请求永不结束，相同问题的后续请求全部阻塞，
// 最终表现为「所有 DNS 解析失败」，且只能重启进程恢复。
//
// 策略（两级）：
//  1. 软恢复：短时间内出现连续失败达到阈值时，自动执行 ClearCache + ResetConnection。
//     ResetConnection 会关闭僵死连接，使 in-flight 请求报错返回，从而删除 singleflight key、
//     解除阻塞。该过程在核心进程内完成，几乎瞬时、无感知。
//  2. 升级处理：若软恢复后短时间内再次触发，说明软恢复无效，执行平台相关的升级动作
//     （见 requestCoreRestart 的 build-tag 实现）：桌面端打印哨兵请求上层重启 sidecar 进程；
//     Android 为进程内运行、无独立进程可重启，执行完整网络状态恢复。带冷却防止恢复风暴。

const (
	// 触发自愈的连续失败次数阈值
	healFailureThreshold = 5
	// 失败计数的滑动窗口
	healFailureWindow = 5 * time.Second
	// 软恢复后的静默期：连接重建需要时间，期间不计失败，避免误升级
	healGracePeriod = 3 * time.Second
	// 软恢复后该时长内再次触发即视为软恢复无效，升级为重启
	healEscalateWindow = 60 * time.Second
	// 两次升级重启请求之间的最小间隔，防止重启风暴
	healEscalateCooldown = 5 * time.Minute
)

type healthMonitor struct {
	mu           sync.Mutex
	failures     []time.Time // 滑动窗口内的失败时间点
	quietUntil   time.Time   // 静默期截止：在此之前不计失败
	lastHealAt   time.Time   // 上次软恢复时间
	lastEscalate time.Time   // 上次升级重启请求时间
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

// trigger 必须在持有锁时调用：决定执行软恢复还是升级重启。
func (h *healthMonitor) trigger(now time.Time) {
	// 软恢复后的升级窗口内再次触发 → 软恢复无效，升级为重启
	if !h.lastHealAt.IsZero() && now.Sub(h.lastHealAt) < healEscalateWindow {
		if !h.lastEscalate.IsZero() && now.Sub(h.lastEscalate) < healEscalateCooldown {
			log.Warnln("[DNS] Automatic recovery was ineffective, but the restart cooldown is active; skipping this restart request")
			return
		}
		h.lastEscalate = now
		h.lastHealAt = time.Time{}
		h.quietUntil = now.Add(healGracePeriod)
		// 升级：软恢复无效，按平台执行升级动作（桌面端重启 sidecar；Android 重置进程内网络状态）
		requestCoreRestart()
		return
	}

	// 首次或距上次软恢复较久：执行软恢复
	h.lastHealAt = now
	h.quietUntil = now.Add(healGracePeriod)
	log.Warnln("[DNS] Consecutive resolution failures detected; starting automatic recovery: clearing cache and resetting upstream connections")
	resolver.ClearCache()
	resolver.ResetConnection()
}
