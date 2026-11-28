package outboundgroup

import (
	"sort"

	C "github.com/metacubex/mihomo/constant"
)

func lessByNameOrder(a, b string, indexByName map[string]int) bool {
	ia, aok := indexByName[a]
	ib, bok := indexByName[b]
	if aok && bok {
		return ia < ib
	}
	if aok != bok {
		return aok
	}
	return false
}

// ReorderCachedProxies puts the GetProxies cache into orderedNames order.
// Names missing from the list keep their relative order at the end.
// The cache slice is replaced so in-flight iterators keep the previous order.
func (gb *GroupBase) ReorderCachedProxies(orderedNames []string) {
	if len(orderedNames) <= 1 {
		return
	}
	_ = gb.GetProxies(false)

	gb.getProxiesMutex.Lock()
	defer gb.getProxiesMutex.Unlock()
	if len(gb.providerProxies) <= 1 {
		return
	}

	indexByName := make(map[string]int, len(orderedNames))
	for i, name := range orderedNames {
		if _, exists := indexByName[name]; !exists {
			indexByName[name] = i
		}
	}
	gb.runtimeOrder = indexByName

	reordered := make([]C.Proxy, len(gb.providerProxies))
	copy(reordered, gb.providerProxies)
	gb.sortRuntimeOrder(reordered)
	gb.providerProxies = reordered
}

// Caller holds getProxiesMutex. Retain the runtime order across provider updates.
func (gb *GroupBase) sortRuntimeOrder(proxies []C.Proxy) {
	if len(gb.runtimeOrder) == 0 {
		return
	}
	sort.SliceStable(proxies, func(i, j int) bool {
		return lessByNameOrder(proxies[i].Name(), proxies[j].Name(), gb.runtimeOrder)
	})
}

// Explicit runtime ordering is an automatic policy, not a manual selection.
// Read current shared health on every choice so groups converge as tests finish.
func (gb *GroupBase) firstRuntimeOrderedHealthy(proxies []C.Proxy, url string) (C.Proxy, bool) {
	gb.getProxiesMutex.Lock()
	ordered := len(gb.runtimeOrder) > 0
	gb.getProxiesMutex.Unlock()
	if !ordered {
		return nil, false
	}
	timeout := gb.TestTimeout
	if timeout <= 0 {
		timeout = 5000
	}
	for _, proxy := range proxies {
		delay := int(proxy.LastDelayForTestUrl(url))
		if proxy.AliveForTestUrl(url) && delay > 0 && delay < timeout {
			return proxy, true
		}
	}
	return nil, true
}
