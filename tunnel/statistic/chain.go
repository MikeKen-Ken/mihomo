package statistic

import C "github.com/metacubex/mihomo/constant"

// chainExcludedFromTunnelTotals reports whether traffic on this connection should not
// be added to DefaultManager global totals (upload/download total and per-second blip).
// The leaf adapter (Chain.Last, i.e. first dial hop) is DIRECT or COMPATIBLE — system direct path.
func chainExcludedFromTunnelTotals(chain C.Chain) bool {
	switch chain.Last() {
	case "DIRECT", "COMPATIBLE":
		return true
	default:
		return false
	}
}
