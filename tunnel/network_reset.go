package tunnel

import (
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
)

// ResetNetworkState discards adapter-owned reusable sessions after the
// underlying network changes. Proxy groups are intentionally skipped;
// only their leaf adapters own transport state.
func ResetNetworkState() int {
	configMux.RLock()
	proxySnapshot := make([]C.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		proxySnapshot = append(proxySnapshot, proxy)
	}
	providerSnapshot := make([]P.ProxyProvider, 0, len(providers))
	for _, provider := range providers {
		providerSnapshot = append(providerSnapshot, provider)
	}
	configMux.RUnlock()

	for _, provider := range providerSnapshot {
		proxySnapshot = append(proxySnapshot, provider.Proxies()...)
	}

	adapters := make([]C.ProxyAdapter, 0, len(proxySnapshot))
	seen := make(map[C.ProxyAdapter]struct{}, len(proxySnapshot))
	for _, proxy := range proxySnapshot {
		if proxy == nil {
			continue
		}
		adapter := proxy.Adapter()
		if _, exists := seen[adapter]; exists {
			continue
		}
		seen[adapter] = struct{}{}
		adapters = append(adapters, adapter)
	}

	reset := 0
	for _, adapter := range adapters {
		resetter, ok := adapter.(C.NetworkStateResetter)
		if !ok {
			continue
		}
		if err := resetter.ResetNetworkState(); err != nil {
			log.Warnln("[Network] reset reusable state for %s failed: %v", adapter.Name(), err)
			continue
		}
		reset++
	}
	return reset
}
