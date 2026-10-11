//go:build !cmfa

package outboundgroup

import "github.com/metacubex/mihomo/adapter"

func notifyHealthCheckTriggered(name string) {}

func notifyMaxConnectTimesTestTriggered(groupName string, proxyName string) {}

func notifyProxyGroupRefresh(groupName string) {}

func NotifyProxyGroupRefresh(groupName string) {
	notifyProxyGroupRefresh(groupName)
}

var recordFailedTimesConnectivity = func(proxyName string, delay int, timeoutMs int) {
	adapter.RecordConnectivitySample(proxyName, delay, timeoutMs)
}
