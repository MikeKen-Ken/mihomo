package resolver

import (
	"net/netip"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
)

const (
	fakeIPMissThreshold  = 6
	fakeIPMissWindow     = 1 * time.Minute
	fakeIPWarnCooldown   = time.Minute
	fakeIPStateRetention = 10 * time.Minute
)

type fakeIPMissState struct {
	count int
	since time.Time
}

type fakeIPRecovery struct {
	mu             sync.Mutex
	byIP           map[netip.Addr]fakeIPMissState
	lastWarn       map[netip.Addr]time.Time
}

var fakeIPRecoveryTracker fakeIPRecovery = fakeIPRecovery{
	byIP:     make(map[netip.Addr]fakeIPMissState),
	lastWarn: make(map[netip.Addr]time.Time),
}

// OnFakeIPRecordMissing 记录反查失败。此时反向映射已经不存在，
// 删除映射不能恢复旧连接，因而只清除 DNS 应答缓存等待客户端重新解析。
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

	ClearCache()
	log.Warnln("[DNS] Fake-IP reverse lookup failed repeatedly; DNS response cache cleared, waiting for the client to resolve again: %s", ip)
}

func (r *fakeIPRecovery) warnRateLimited(ip netip.Addr) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked(now)

	if last, ok := r.lastWarn[ip]; ok && now.Sub(last) < fakeIPWarnCooldown {
		return
	}
	r.lastWarn[ip] = now
	log.Warnln("[DNS] Fake-IP reverse lookup failed: %s (the mapping may have expired; resolving again on the client will recover it)", ip)
}

func (r *fakeIPRecovery) recordAndShouldRepair(ip netip.Addr) bool {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

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
	return true
}

func (r *fakeIPRecovery) cleanupLocked(now time.Time) {
	for ip, last := range r.lastWarn {
		if now.Sub(last) > fakeIPStateRetention {
			delete(r.lastWarn, ip)
			delete(r.byIP, ip)
		}
	}
}
