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
