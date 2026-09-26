//go:build !cmfa

package outboundgroup

import "github.com/metacubex/mihomo/adapter"

func notifyHealthCheckTriggered(name string) {}

func notifyMaxConnectTimesTestTriggered(groupName string, proxyName string) {}

func notifyProxyGroupRefresh(groupName string) {}

var recordFailedTimesConnectivity = func(proxyName string, delay int, timeoutMs int) {
	adapter.RecordConnectivitySample(proxyName, delay, timeoutMs)
}
