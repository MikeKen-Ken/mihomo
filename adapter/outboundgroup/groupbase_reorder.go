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
		indexByName[name] = i
	}

	reordered := make([]C.Proxy, len(gb.providerProxies))
	copy(reordered, gb.providerProxies)
	sort.SliceStable(reordered, func(i, j int) bool {
		return lessByNameOrder(reordered[i].Name(), reordered[j].Name(), indexByName)
	})
	gb.providerProxies = reordered
}
