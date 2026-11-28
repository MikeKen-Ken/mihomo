//go:build !cmfa

package adapter

func recordProxyConnectivityTest(proxyName string, delay int, timeoutMs int) {
	recordDesktopConnectivityStats(proxyName, delay, timeoutMs)
}
