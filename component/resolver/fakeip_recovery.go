package resolver

import (
	"net/netip"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
)

const (
	fakeIPMissThreshold     = 3
	fakeIPMissWindow        = 30 * time.Second
	fakeIPAutoFlushCooldown = 60 * time.Second
	fakeIPWarnCooldown      = time.Minute
)

type fakeIPMissState struct {
	count int
	since time.Time
}

type fakeIPRecovery struct {
	mu            sync.Mutex
	byIP          map[netip.Addr]fakeIPMissState
	lastWarn      map[netip.Addr]time.Time
	lastAutoFlush time.Time
}

var fakeIPRecoveryTracker fakeIPRecovery = fakeIPRecovery{
	byIP:     make(map[netip.Addr]fakeIPMissState),
	lastWarn: make(map[netip.Addr]time.Time),
}

// OnFakeIPRecordMissing records a reverse-lookup failure, logs a rate-limited warn,
// and auto-flushes Fake-IP + DNS cache when the same IP misses repeatedly.
func OnFakeIPRecordMissing(ip netip.Addr) {
	if !IsFakeIP(ip) {
		return
	}
	fakeIPRecoveryTracker.onMiss(ip)
}

func (r *fakeIPRecovery) onMiss(ip netip.Addr) {
	r.warnRateLimited(ip)

	if !r.recordAndShouldFlush(ip) {
		return
	}

	if err := FlushFakeIP(); err != nil {
		log.Warnln("[DNS] Fake-IP 自动修复失败: %v", err)
		return
	}

	log.Warnln("[DNS] Fake-IP 反查连续失败，已自动清空 Fake-IP 映射与 DNS 缓存")
}

func (r *fakeIPRecovery) warnRateLimited(ip netip.Addr) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	if last, ok := r.lastWarn[ip]; ok && now.Sub(last) < fakeIPWarnCooldown {
		return
	}
	r.lastWarn[ip] = now
	log.Warnln("[DNS] Fake-IP 反查失败: %s（映射可能已过期；若持续失败将自动刷新缓存）", ip)
}

func (r *fakeIPRecovery) recordAndShouldFlush(ip netip.Addr) bool {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.lastAutoFlush.IsZero() && now.Sub(r.lastAutoFlush) < fakeIPAutoFlushCooldown {
		return false
	}

	state, ok := r.byIP[ip]
	if !ok || now.Sub(state.since) > fakeIPMissWindow {
		r.byIP[ip] = fakeIPMissState{count: 1, since: now}
		return false
	}

	state.count++
	r.byIP[ip] = state
	if state.count < fakeIPMissThreshold {
		return false
	}

	delete(r.byIP, ip)
	r.lastAutoFlush = now
	return true
}
