package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type urlTestOption func(*URLTest)

func urlTestWithTolerance(tolerance uint16) urlTestOption {
	return func(u *URLTest) {
		u.tolerance = tolerance
	}
}

type URLTest struct {
	*GroupBase
	selection      manualSelectionState
	testUrl        string
	expectedStatus string
	tolerance      uint16
	disableUDP     bool
	fastNodeMux    sync.Mutex
	fastNode       C.Proxy
	fastSingle     *singledo.Single[C.Proxy]
}

func (u *URLTest) Now() string {
	return u.fast(false).Name()
}

func (u *URLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}
	if p == nil {
		return errors.New("proxy not exist")
	}
	u.ForceSet(name)
	return nil
}

func (u *URLTest) ForceSet(name string) {
	u.selection.set(name)
	u.fastSingle.Reset()
}

// ClearManualSelection releases the pin and drops the cached fast node so
// the next Dial uses live auto-select instead of the previously pinned proxy.
func (u *URLTest) ClearManualSelection() {
	u.selection.clear()
	u.fastNodeMux.Lock()
	u.fastNode = nil
	u.fastNodeMux.Unlock()
	u.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (u *URLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy, selection := u.fastWithSelection(true)
	callbacks := proxyPrecheckCallbacks{
		onSuccess: func() {
			u.clearManualSelectionIfUnchanged(selection)
		},
		onFailure: func() {
			u.healthCheckForSelection(selection)
		},
	}
	c, err = proxy.DialContext(ctx, metadata)
	needHandshake := err == nil && N.NeedHandshake(c)
	if err == nil {
		c.AppendToChains(u)
	} else {
		u.onDialFailedWithCallbacks(ctx, proxy.Type(), err, proxy, u.testUrl, u.expectedStatus, callbacks)
	}

	if needHandshake {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				u.onDialSuccess()
			} else {
				u.onDialFailedWithCallbacks(ctx, proxy.Type(), err, proxy, u.testUrl, u.expectedStatus, callbacks)
			}
		})
	}
	if err == nil {
		c = u.observePostConnectFailureWithCallbacks(ctx, c, proxy.Type(), proxy, u.testUrl, u.expectedStatus, needHandshake, callbacks)
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy, selection := u.fastWithSelection(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(u)
	} else {
		u.onDialFailedWithCallbacks(ctx, proxy.Type(), err, proxy, u.testUrl, u.expectedStatus, proxyPrecheckCallbacks{
			onSuccess: func() {
				u.clearManualSelectionIfUnchanged(selection)
			},
			onFailure: func() {
				u.healthCheckForSelection(selection)
			},
		})
	}

	return pc, err
}

func (u *URLTest) CountRequest(metadata *C.Metadata) {
	proxy, selection := u.fastWithSelection(true)
	u.onRequestAttempt(proxy, u.testUrl, u.expectedStatus, func() {
		u.healthCheckForSelection(selection)
	})
}

// Unwrap implements C.ProxyAdapter
func (u *URLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
}

func (u *URLTest) healthCheck() {
	u.healthCheckForSelection(u.selection.snapshot())
}

func (u *URLTest) healthCheckForSelection(selection manualSelectionSnapshot) {
	u.clearManualSelectionIfUnchanged(selection)
	u.GroupBase.healthCheck(u.testUrl, u.expectedStatus)
	u.fastSingle.Reset()
}

// NowIsManual implements NowIsManualAble.
func (u *URLTest) NowIsManual() bool {
	return u.selection.snapshot().name != ""
}

func (u *URLTest) clearManualSelectionIfUnchanged(selection manualSelectionSnapshot) {
	if !u.selection.clearIfUnchanged(selection) {
		return
	}
	u.fastNodeMux.Lock()
	u.fastNode = nil
	u.fastNodeMux.Unlock()
	u.fastSingle.Reset()
	u.onManualSelectionCleared()
}

func (u *URLTest) fastWithSelection(touch bool) (C.Proxy, manualSelectionSnapshot) {
	proxy := u.fast(touch)
	return proxy, u.selection.snapshot()
}

func (u *URLTest) fast(touch bool) C.Proxy {
	elm, _, shared := u.fastSingle.Do(func() (C.Proxy, error) {
		u.fastNodeMux.Lock()
		proxies := u.GetProxies(touch)
		// 手动选择：立即生效，不等待该节点测速完成（与 Fallback 一致）
		selection := u.selection.snapshot()
		clearedSelection := false
		if selection.name != "" {
			for _, proxy := range proxies {
				if proxy.Name() == selection.name {
					u.fastNode = proxy
					u.fastNodeMux.Unlock()
					return proxy, nil
				}
			}
			clearedSelection = u.selection.clearIfUnchanged(selection)
			u.fastNode = nil
		}

		fast := proxies[0]
		minDelay := fast.LastDelayForTestUrl(u.testUrl)
		fastNotExist := true

		for _, proxy := range proxies[1:] {
			if u.fastNode != nil && proxy.Name() == u.fastNode.Name() {
				fastNotExist = false
			}

			if !proxy.AliveForTestUrl(u.testUrl) {
				continue
			}

			delay := proxy.LastDelayForTestUrl(u.testUrl)
			if delay < minDelay {
				fast = proxy
				minDelay = delay
			}

		}
		// tolerance
		if u.fastNode == nil || fastNotExist || !u.fastNode.AliveForTestUrl(u.testUrl) || u.fastNode.LastDelayForTestUrl(u.testUrl) > fast.LastDelayForTestUrl(u.testUrl)+u.tolerance {
			u.fastNode = fast
		}
		result := u.fastNode
		u.fastNodeMux.Unlock()
		if clearedSelection {
			u.onManualSelectionCleared()
		}
		return result, nil
	})
	if shared && touch { // a shared fastSingle.Do() may cause providers untouched, so we touch them again
		u.Touch()
	}

	return elm
}

// SupportUDP implements C.ProxyAdapter
func (u *URLTest) SupportUDP() bool {
	if u.disableUDP {
		return false
	}
	return u.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (u *URLTest) IsL3Protocol(metadata *C.Metadata) bool {
	return u.fast(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (u *URLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":            u.Type().String(),
		"now":             u.Now(),
		"all":             all,
		"testUrl":         u.testUrl,
		"expectedStatus":  u.expectedStatus,
		"fixed":           u.selection.snapshot().name,
		"hidden":          u.Hidden(),
		"icon":            u.Icon(),
		"connectTimes":    u.ConnectTimes(),
		"maxConnectTimes": u.MaxConnectTimes(),
	})
}

func (u *URLTest) Providers() []P.ProxyProvider {
	return u.providers
}

func (u *URLTest) Proxies() []C.Proxy {
	return u.GetProxies(false)
}

func (u *URLTest) DelayTestSpec() (string, string) {
	return u.testUrl, u.expectedStatus
}

func (u *URLTest) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return u.GroupBase.URLTest(ctx, u.testUrl, expectedStatus)
}

func parseURLTestOption(config map[string]any) []urlTestOption {
	opts := []urlTestOption{}

	// tolerance
	if elm, ok := config["tolerance"]; ok {
		if tolerance, ok := elm.(int); ok {
			opts = append(opts, urlTestWithTolerance(uint16(tolerance)))
		}
	}

	return opts
}

func NewURLTest(option *GroupCommonOption, providers []P.ProxyProvider, options ...urlTestOption) *URLTest {
	urlTest := &URLTest{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:                 option.Name,
			Type:                 C.URLTest,
			Hidden:               option.Hidden,
			Icon:                 option.Icon,
			Filter:               option.Filter,
			ExcludeFilter:        option.ExcludeFilter,
			ExcludeType:          option.ExcludeType,
			TestTimeout:          option.TestTimeout,
			FailureResetInterval: option.FailureResetInterval,
			MaxFailedTimes:       option.MaxFailedTimes,
			MaxConnectTimes:      option.MaxConnectTimes,
			Providers:            providers,
		}),
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 10),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
	}

	for _, option := range options {
		option(urlTest)
	}

	return urlTest
}
