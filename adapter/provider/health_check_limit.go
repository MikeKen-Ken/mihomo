package provider

import (
	"context"
	"sync"
)

const healthCheckWorkerLimitMax = 30

var (
	healthCheckWorkerLimitMu sync.RWMutex
	healthCheckWorkerLimitN  = 30
	healthCheckWorkerActive  int
	healthCheckWorkerChanged = make(chan struct{})
)

// SetHealthCheckWorkerLimit 设置订阅 Provider 自动健康检查的并发上限（与 errgroup.SetLimit 一致）。
// n <= 0 时回退为 30；过大时截断以保护低端设备。
func SetHealthCheckWorkerLimit(n int) {
	healthCheckWorkerLimitMu.Lock()
	healthCheckWorkerLimitN = ClampHealthCheckWorkerLimit(n)
	notifyHealthCheckWorkerChangedLocked()
	healthCheckWorkerLimitMu.Unlock()
}

func ClampHealthCheckWorkerLimit(n int) int {
	if n <= 0 {
		return 30
	}
	if n > healthCheckWorkerLimitMax {
		return healthCheckWorkerLimitMax
	}
	return n
}

// EffectiveHealthCheckWorkerLimit 返回当前健康检查并发上限。
func EffectiveHealthCheckWorkerLimit() int {
	healthCheckWorkerLimitMu.RLock()
	defer healthCheckWorkerLimitMu.RUnlock()
	if healthCheckWorkerLimitN <= 0 {
		return 30
	}
	return healthCheckWorkerLimitN
}

// AcquireHealthCheckWorker shares one process-wide ceiling between automatic
// provider checks and user-triggered group delay tests.
func AcquireHealthCheckWorker(ctx context.Context) bool {
	for {
		healthCheckWorkerLimitMu.Lock()
		limit := healthCheckWorkerLimitN
		if limit <= 0 {
			limit = 30
		}
		if healthCheckWorkerActive < limit {
			healthCheckWorkerActive++
			healthCheckWorkerLimitMu.Unlock()
			return true
		}
		changed := healthCheckWorkerChanged
		healthCheckWorkerLimitMu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

func ReleaseHealthCheckWorker() {
	healthCheckWorkerLimitMu.Lock()
	if healthCheckWorkerActive <= 0 {
		healthCheckWorkerLimitMu.Unlock()
		panic("release health check worker without acquire")
	}
	healthCheckWorkerActive--
	notifyHealthCheckWorkerChangedLocked()
	healthCheckWorkerLimitMu.Unlock()
}

func notifyHealthCheckWorkerChangedLocked() {
	close(healthCheckWorkerChanged)
	healthCheckWorkerChanged = make(chan struct{})
}
