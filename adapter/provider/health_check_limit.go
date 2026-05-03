package provider

import "sync"

const healthCheckWorkerLimitMax = 256

var (
	healthCheckWorkerLimitMu sync.RWMutex
	healthCheckWorkerLimitN  = 30
)

// SetHealthCheckWorkerLimit 设置订阅 Provider 自动健康检查的并发上限（与 errgroup.SetLimit 一致）。
// n <= 0 时回退为 30；过大时截断以保护低端设备。
func SetHealthCheckWorkerLimit(n int) {
	healthCheckWorkerLimitMu.Lock()
	defer healthCheckWorkerLimitMu.Unlock()
	if n <= 0 {
		healthCheckWorkerLimitN = 30
		return
	}
	if n > healthCheckWorkerLimitMax {
		n = healthCheckWorkerLimitMax
	}
	healthCheckWorkerLimitN = n
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
