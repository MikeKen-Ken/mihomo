package outboundgroup

import (
	"context"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type ProxyGroup interface {
	C.ProxyAdapter

	Providers() []P.ProxyProvider
	Proxies() []C.Proxy
	Now() string
	Touch()

	Hidden() bool
	Icon() string

	URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (mp map[string]uint16, err error)
}

// DelayTestSpecAble exposes the URL and expected status owned by a proxy group.
// Callers that test group members must use this specification instead of a
// provider health-check URL so the result matches the group's own selection.
type DelayTestSpecAble interface {
	DelayTestSpec() (url string, expectedStatus string)
}

var _ ProxyGroup = (*Fallback)(nil)
var _ ProxyGroup = (*LoadBalance)(nil)
var _ ProxyGroup = (*URLTest)(nil)
var _ ProxyGroup = (*Selector)(nil)
var _ DelayTestSpecAble = (*Fallback)(nil)
var _ DelayTestSpecAble = (*LoadBalance)(nil)
var _ DelayTestSpecAble = (*URLTest)(nil)
var _ DelayTestSpecAble = (*Selector)(nil)

type SelectAble interface {
	Set(string) error
	ForceSet(name string)
}

// ClearManualSelectionAble is implemented by groups that remember a manual selection.
// When health check (测速) is triggered, clearing means "all nodes not manually selected".
type ClearManualSelectionAble interface {
	ClearManualSelection()
}

// NowIsManualAble is implemented by groups that can have a "manual" current selection.
// When false, UI should not show any node as "current"; traffic still uses first/auto node.
type NowIsManualAble interface {
	NowIsManual() bool
}

type ConnectTimesAble interface {
	ConnectTimes() int
	MaxConnectTimes() int
	ResetConnectTimes()
}

var _ SelectAble = (*Fallback)(nil)
var _ SelectAble = (*URLTest)(nil)
var _ SelectAble = (*Selector)(nil)
var _ ClearManualSelectionAble = (*Selector)(nil)
var _ ClearManualSelectionAble = (*Fallback)(nil)
var _ ClearManualSelectionAble = (*URLTest)(nil)
var _ NowIsManualAble = (*Selector)(nil)
var _ NowIsManualAble = (*Fallback)(nil)
var _ NowIsManualAble = (*URLTest)(nil)
var _ ConnectTimesAble = (*GroupBase)(nil)
