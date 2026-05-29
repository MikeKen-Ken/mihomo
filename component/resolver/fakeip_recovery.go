package resolver

import (
	"net/netip"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
)

const (
	fakeIPMissThreshold      = 6
	fakeIPMissWindow         = 1 * time.Minute
	fakeIPAutoRepairCooldown = 60 * time.Second
	fakeIPWarnCooldown       = time.Minute
)

type fakeIPMissState struct {
	count int
	since time.Time
}

type fakeIPRecovery struct {
	mu             sync.Mutex
	byIP           map[netip.Addr]fakeIPMissState
	lastWarn       map[netip.Addr]time.Time
	lastAutoRepair time.Time
}

var fakeIPRecoveryTracker fakeIPRecovery = fakeIPRecovery{
	byIP:     make(map[netip.Addr]fakeIPMissState),
	lastWarn: make(map[netip.Addr]time.Time),
}

// OnFakeIPRecordMissing records a reverse-lookup failure, logs a rate-limited warn,
// and removes only that IP's mapping when the same address misses repeatedly.
func OnFakeIPRecordMissing(ip netip.Addr) {
	if !IsFakeIP(ip) {
		return
	}
	fakeIPRecoveryTracker.onMiss(ip)
}

func (r *fakeIPRecovery) onMiss(ip netip.Addr) {
	r.warnRateLimited(ip)

	if !r.recordAndShouldRepair(ip) {
		return
	}

	DeleteFakeIPMapping(ip)
	log.Warnln("[DNS] Fake-IP 反查连续失败，已移除 %s 的映射（未清空全局 Fake-IP 表）", ip)
}

func (r *fakeIPRecovery) warnRateLimited(ip netip.Addr) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	if last, ok := r.lastWarn[ip]; ok && now.Sub(last) < fakeIPWarnCooldown {
		return
	}
	r.lastWarn[ip] = now
	log.Warnln("[DNS] Fake-IP 反查失败: %s（映射可能已过期；若持续失败将仅移除该 IP 的映射）", ip)
}

func (r *fakeIPRecovery) recordAndShouldRepair(ip netip.Addr) bool {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.lastAutoRepair.IsZero() && now.Sub(r.lastAutoRepair) < fakeIPAutoRepairCooldown {
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
	r.lastAutoRepair = now
	return true
}
