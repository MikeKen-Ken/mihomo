//go:build cfa

package outboundgroup

import "cfa/native/delegate"

func notifyHealthCheckTriggered(name string) {
	delegate.NotifyHealthCheckTriggered(name)
}
