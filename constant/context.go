package constant

import (
	"context"
	"net"

	N "github.com/metacubex/mihomo/common/net"

	"github.com/gofrs/uuid/v5"
)

type ctxKeySuppressGroupOutboundFailureStats struct{}

// WithSuppressGroupOutboundFailureStats 标记 ctx：策略组 outbound 的拨号失败不计入 max-failed-times（用于测速 URLTest、健康检查等）。
func WithSuppressGroupOutboundFailureStats(parent context.Context) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithValue(parent, ctxKeySuppressGroupOutboundFailureStats{}, true)
}

// SuppressGroupOutboundFailureStats 是否应跳过策略组拨号失败统计。
func SuppressGroupOutboundFailureStats(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeySuppressGroupOutboundFailureStats{}).(bool)
	return v
}

type ctxKeyHealthCheckSourceName struct{}

// WithHealthCheckSourceName 在 ctx 上附带健康检查来源标识（如策略组名、provider 名），供 URLTest 日志输出。
func WithHealthCheckSourceName(parent context.Context, name string) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	if name == "" {
		return parent
	}
	return context.WithValue(parent, ctxKeyHealthCheckSourceName{}, name)
}

// HealthCheckSourceName 返回 WithHealthCheckSourceName 设置的名称；未设置时为空字符串。
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
