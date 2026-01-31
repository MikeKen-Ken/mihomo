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

	URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (mp map[string]uint16, err error)
}

var _ ProxyGroup = (*Fallback)(nil)
var _ ProxyGroup = (*LoadBalance)(nil)
var _ ProxyGroup = (*URLTest)(nil)
var _ ProxyGroup = (*Selector)(nil)

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

var _ SelectAble = (*Fallback)(nil)
var _ SelectAble = (*URLTest)(nil)
var _ SelectAble = (*Selector)(nil)
var _ ClearManualSelectionAble = (*Selector)(nil)
var _ ClearManualSelectionAble = (*Fallback)(nil)
var _ NowIsManualAble = (*Selector)(nil)
var _ NowIsManualAble = (*Fallback)(nil)
