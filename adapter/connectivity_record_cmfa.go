//go:build cmfa

package adapter

import "cfa/native/delegate"

func recordProxyConnectivityTest(proxyName string, delay int, timeoutMs int) {
	delegate.RecordProxyConnectivityTest(proxyName, delay, timeoutMs)
}
