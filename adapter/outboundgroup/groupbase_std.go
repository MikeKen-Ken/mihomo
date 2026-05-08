//go:build !cmfa

package outboundgroup

func notifyHealthCheckTriggered(name string) {}

func notifyMaxConnectTimesTestTriggered(groupName string, proxyName string) {}
