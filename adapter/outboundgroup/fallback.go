package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type Fallback struct {
	*GroupBase
	disableUDP      bool
	testUrl         string
	selected        string
	expectedStatus  string
	selectedTimeout int // ms, for selected node only; 0 = use same as normal (AliveForTestUrl)
	Hidden          bool
	Icon            string
}

func (f *Fallback) Now() string {
	proxy := f.findAliveProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := f.findAliveProxy(true)
	c, err := proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(f)
	} else {
		f.onDialFailed(proxy.Type(), err, f.healthCheck)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				f.onDialSuccess()
			} else {
				f.onDialFailed(proxy.Type(), err, f.healthCheck)
			}
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (f *Fallback) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := f.findAliveProxy(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(f)
	}

	return pc, err
}

// SupportUDP implements C.ProxyAdapter
func (f *Fallback) SupportUDP() bool {
	if f.disableUDP {
		return false
	}

	proxy := f.findAliveProxy(false)
	return proxy.SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (f *Fallback) IsL3Protocol(metadata *C.Metadata) bool {
	return f.findAliveProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (f *Fallback) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range f.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":            f.Type().String(),
		"now":             f.Now(),
		"all":             all,
		"testUrl":         f.testUrl,
		"expectedStatus": f.expectedStatus,
		"fixed":           f.selected,
		"selectedTimeout": f.selectedTimeout,
		"hidden":          f.Hidden,
		"icon":            f.Icon,
	})
}

// Unwrap implements C.ProxyAdapter
func (f *Fallback) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := f.findAliveProxy(touch)
	return proxy
}

func (f *Fallback) findAliveProxy(touch bool) C.Proxy {
	proxies := f.GetProxies(touch)
	timeoutMs := f.TestTimeout
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	for _, proxy := range proxies {
		if len(f.selected) == 0 {
			// Only use proxy if alive and delay is within group timeout
			if proxy.AliveForTestUrl(f.testUrl) && proxy.LastDelayForTestUrl(f.testUrl) <= uint16(timeoutMs) {
				return proxy
			}
		} else {
			if proxy.Name() == f.selected {
				alive := false
				if f.selectedTimeout > 0 {
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(f.selectedTimeout))
					defer cancel()
					expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
					_, err := proxy.URLTest(ctx, f.testUrl, expectedStatus)
					alive = err == nil
				} else {
					alive = proxy.AliveForTestUrl(f.testUrl)
				}
				if alive {
					return proxy
				}
				f.selected = ""
			}
		}
	}

	// No proxy is alive within timeout: use the one with minimum delay (least bad) instead of always the first
	var best C.Proxy
	minDelay := uint16(0xffff)
	for _, proxy := range proxies {
		d := proxy.LastDelayForTestUrl(f.testUrl)
		if d < minDelay {
			minDelay = d
			best = proxy
		}
	}
	if best != nil {
		return best
	}
	return proxies[0]
}

func (f *Fallback) Set(name string) error {
	var p C.Proxy
	for _, proxy := range f.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	f.selected = name
	// 固定测速：选中节点时始终执行一次 URLTest，更新延迟与存活状态
	timeoutMs := f.selectedTimeout
	if timeoutMs <= 0 {
		timeoutMs = f.TestTimeout
	}
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(timeoutMs))
	defer cancel()
	expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
	_, _ = p.URLTest(ctx, f.testUrl, expectedStatus)

	return nil
}

func (f *Fallback) ForceSet(name string) {
	f.selected = name
}

// NowIsManual implements NowIsManualAble.
func (f *Fallback) NowIsManual() bool {
	return f.selected != ""
}

// ClearManualSelection clears the fixed selected node so the group auto-picks first alive.
func (f *Fallback) ClearManualSelection() {
	f.selected = ""
}

func (f *Fallback) Providers() []P.ProxyProvider {
	return f.providers
}

func (f *Fallback) Proxies() []C.Proxy {
	return f.GetProxies(false)
}

func NewFallback(option *GroupCommonOption, providers []P.ProxyProvider) *Fallback {
	return &Fallback{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:                 option.Name,
			Type:                 C.Fallback,
			Filter:               option.Filter,
			ExcludeFilter:        option.ExcludeFilter,
			ExcludeType:          option.ExcludeType,
			TestTimeout:          option.TestTimeout,
			FailureResetInterval: option.FailureResetInterval,
			MaxFailedTimes:       option.MaxFailedTimes,
			Providers:            providers,
		}),
		disableUDP:      option.DisableUDP,
		testUrl:         option.URL,
		expectedStatus:  option.ExpectedStatus,
		selectedTimeout: option.SelectedTimeout,
		Hidden:          option.Hidden,
		Icon:            option.Icon,
	}
}
