//go:build cmfa

package outboundgroup

import "cfa/native/delegate"

func notifyHealthCheckTriggered(name string) {
	delegate.NotifyHealthCheckTriggered(name)
}

func notifyMaxConnectTimesTestTriggered(groupName string, proxyName string) {
	delegate.NotifyHealthCheckTriggered(maxConnectTimesTestEventPrefix + groupName + "\t" + proxyName)
}

func notifyProxyGroupRefresh(groupName string) {
	delegate.NotifyHealthCheckTriggered(proxyGroupRefreshEventPrefix + groupName)
}
