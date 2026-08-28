package dns

import "sync"

var (
	networkRecoveryMu sync.RWMutex
	networkRecoveryFn func(string)
)

// SetNetworkRecoveryFunc installs the platform-owned full recovery action.
// It is used only when DNS connection reset has already failed.
func SetNetworkRecoveryFunc(fn func(string)) {
	networkRecoveryMu.Lock()
	networkRecoveryFn = fn
	networkRecoveryMu.Unlock()
}

func requestNetworkRecovery(reason string) bool {
	networkRecoveryMu.RLock()
	fn := networkRecoveryFn
	networkRecoveryMu.RUnlock()
	if fn == nil {
		return false
	}
	fn(reason)
	return true
}
