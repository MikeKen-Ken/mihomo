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
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type Fallback struct {
	*GroupBase
	disableUDP      bool
	testUrl         string
	selected        string // 手动选择的节点；仅当用户执行「组测速」时由 ClearManualSelection 清空，健康检测/连接失败不触发 fallback
	expectedStatus  string
	selectedTimeout int // ms, for selected node only; 0 = use same as normal (AliveForTestUrl)
}

func (f *Fallback) Now() string {
	proxy := f.findAliveProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := f.findAliveProxy(true)
	healthCheck := func() {
		f.healthCheckForProxy(proxy)
	}
	f.onDialAttempt(proxy, f.testUrl, f.expectedStatus, healthCheck)
	c, err := proxy.DialContext(ctx, metadata)
	needHandshake := err == nil && N.NeedHandshake(c)
	if err == nil {
		c.AppendToChains(f)
	} else {
		f.onDialFailed(proxy.Type(), err, healthCheck)
	}

	if needHandshake {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				f.onDialSuccess()
			} else {
				f.onDialFailed(proxy.Type(), err, healthCheck)
			}
		})
	}
	if err == nil {
		c = f.observePostConnectFailure(c, proxy.Type(), needHandshake, healthCheck)
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (f *Fallback) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := f.findAliveProxy(true)
	f.onDialAttempt(proxy, f.testUrl, f.expectedStatus, func() {
		f.healthCheckForProxy(proxy)
	})
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(f)
	}

	return pc, err
}

func (f *Fallback) healthCheck() {
	f.healthCheckForProxy(f.findAliveProxy(false))
}

func (f *Fallback) healthCheckForProxy(proxy C.Proxy) {
	if proxy == nil {
		f.GroupBase.healthCheck()
		statistic.DefaultManager.CloseConnectionsUsingProxyGroup(f.Name())
		return
	}

	proxyName := proxy.Name()
	groups := f.fallbackGroupsUsingProxy(proxyName)
	groupNames := make([]string, 0, len(groups))
	for _, group := range groups {
		group.GroupBase.healthCheck()
		groupNames = append(groupNames, group.Name())
	}
	statistic.DefaultManager.CloseConnectionsUsingProxyGroupsAndProxy(groupNames, proxyName)
}

func (f *Fallback) fallbackGroupsUsingProxy(proxyName string) []*Fallback {
	groups := []*Fallback{f}
	seen := map[*Fallback]struct{}{f: {}}
	for _, proxy := range tunnel.Proxies() {
		group, ok := proxy.Adapter().(*Fallback)
		if !ok {
			continue
		}
		if _, ok := seen[group]; ok {
			continue
		}
		current := group.findAliveProxy(false)
		if current == nil || current.Name() != proxyName {
			continue
		}
		seen[group] = struct{}{}
		groups = append(groups, group)
	}
	return groups
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
		"hidden":          f.Hidden(),
		"icon":            f.Icon(),
		"connectTimes":     f.ConnectTimes(),
		"maxConnectTimes": f.MaxConnectTimes(),
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

	// 手动选择模式：无条件返回用户选择的节点
	// 健康检测由 Set() 的异步 goroutine 负责，这里不阻止用户的选择
	if len(f.selected) > 0 {
		for _, proxy := range proxies {
			if proxy.Name() == f.selected {
				return proxy // 直接返回，不检查延迟或健康状态
			}
		}
		// 选择的节点不存在于列表中（可能被移除），清空选择
		f.selected = ""
	}

	// 自动模式：返回第一个可用的节点
	for _, proxy := range proxies {
		// Only use proxy if alive and delay is within group timeout
		if proxy.AliveForTestUrl(f.testUrl) && proxy.LastDelayForTestUrl(f.testUrl) <= uint16(timeoutMs) {
			return proxy
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

	// 立即切换到用户选择的节点并固定使用，  不因健康检测/连接失败而触发 fallback；仅当用户执行「组测速」时由 ClearManualSelection 清空
	f.selected = name

	// 异步健康检测：仅用于更新延迟显示，不根据结果修改 selected
	go func() {
		defer f.resetConnectTimes()

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
	}()

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
	selectedTimeout := option.SelectedTimeout
	if selectedTimeout <= 0 {
		selectedTimeout = option.TestTimeout
	}
	if selectedTimeout <= 0 {
		selectedTimeout = 5000
	}

	return &Fallback{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:                 option.Name,
			Type:                 C.Fallback,
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
		disableUDP:      option.DisableUDP,
		testUrl:         option.URL,
		expectedStatus:  option.ExpectedStatus,
		selectedTimeout: selectedTimeout,
	}
}
