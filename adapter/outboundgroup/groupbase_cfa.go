//go:build cmfa

package outboundgroup

import (
	"encoding/json"

	"cfa/native/delegate"
)

const healthCheckEventVersion = 1

type healthCheckEvent struct {
	V     int    `json:"v"`
	Type  string `json:"type"`
	Group string `json:"group"`
	Proxy string `json:"proxy,omitempty"`
}

func marshalHealthCheckEvent(eventType, groupName, proxyName string) string {
	payload, err := json.Marshal(healthCheckEvent{
		V:     healthCheckEventVersion,
		Type:  eventType,
		Group: groupName,
		Proxy: proxyName,
	})
	if err != nil {
		// Extremely unlikely; fall back to legacy tab protocol for this notify only.
		switch eventType {
		case "max-connect-times":
			return maxConnectTimesTestEventPrefix + groupName + "\t" + proxyName
		case "proxy-group-refresh":
			return proxyGroupRefreshEventPrefix + groupName
		default:
			return groupName
		}
	}
	return string(payload)
}

func notifyHealthCheckTriggered(name string) {
	delegate.NotifyHealthCheckTriggered(marshalHealthCheckEvent("health-check", name, ""))
}

func notifyMaxConnectTimesTestTriggered(groupName string, proxyName string) {
	delegate.NotifyHealthCheckTriggered(marshalHealthCheckEvent("max-connect-times", groupName, proxyName))
}

func notifyProxyGroupRefresh(groupName string) {
	delegate.NotifyHealthCheckTriggered(marshalHealthCheckEvent("proxy-group-refresh", groupName, ""))
}
