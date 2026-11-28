package constant

import (
	"context"
	"net"

	N "github.com/metacubex/mihomo/common/net"

	"github.com/gofrs/uuid/v5"
)

type ctxKeySuppressGroupOutboundFailureStats struct{}

// WithSuppressGroupOutboundFailureStats marks ctx so group outbound dial failures
// are not counted toward max-failed-times during delay tests or health checks.
func WithSuppressGroupOutboundFailureStats(parent context.Context) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithValue(parent, ctxKeySuppressGroupOutboundFailureStats{}, true)
}

// SuppressGroupOutboundFailureStats reports whether group dial failure stats should be skipped.
func SuppressGroupOutboundFailureStats(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeySuppressGroupOutboundFailureStats{}).(bool)
	return v
}

type ctxKeyHealthCheckSourceName struct{}

// WithHealthCheckSourceName attaches a source name (group or provider) to ctx for URLTest logging.
func WithHealthCheckSourceName(parent context.Context, name string) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	if name == "" {
		return parent
	}
	return context.WithValue(parent, ctxKeyHealthCheckSourceName{}, name)
}

// HealthCheckSourceName returns the source name attached by WithHealthCheckSourceName.
func HealthCheckSourceName(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	s, _ := ctx.Value(ctxKeyHealthCheckSourceName{}).(string)
	return s
}

type ctxKeyDelayTestTimeoutMs struct{}

// WithDelayTestTimeoutMs records the delay-test timeout. Displayed delay and
// the single URLTest deadline both use this value: dial start through first HTTP.
func WithDelayTestTimeoutMs(parent context.Context, timeoutMs int) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	if timeoutMs <= 0 {
		return parent
	}
	return context.WithValue(parent, ctxKeyDelayTestTimeoutMs{}, timeoutMs)
}

// DelayTestTimeoutMs returns the delay-test timeout, if one was set.
func DelayTestTimeoutMs(ctx context.Context) (int, bool) {
	if ctx == nil {
		return 0, false
	}
	timeoutMs, ok := ctx.Value(ctxKeyDelayTestTimeoutMs{}).(int)
	return timeoutMs, ok && timeoutMs > 0
}

type PlainContext interface {
	ID() uuid.UUID
}

type ConnContext interface {
	PlainContext
	Metadata() *Metadata
	Conn() *N.BufferedConn
}

type PacketConnContext interface {
	PlainContext
	Metadata() *Metadata
	PacketConn() net.PacketConn
}
