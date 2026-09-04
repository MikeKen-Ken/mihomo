package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type Fallback struct {
	*GroupBase
	disableUDP      bool
	testUrl         string
	selection       manualSelectionState
	expectedStatus  string
	selectedTimeout int // ms, for selected node only; 0 = use same as normal (AliveForTestUrl)
}

func (f *Fallback) Now() string {
	proxy := f.findAliveProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy, selection := f.findAliveProxyWithSelection(true)
	callbacks := proxyPrecheckCallbacks{
		onSuccess: func() {
			f.clearManualSelectionIfUnchanged(selection)
		},
		onFailure: func() {
			f.healthCheckForProxy(proxy, selection)
		},
	}
	c, err := proxy.DialContext(ctx, metadata)
	needHandshake := err == nil && N.NeedHandshake(c)
	if err == nil {
		c.AppendToChains(f)
	} else {
		f.onDialFailedWithCallbacks(ctx, proxy.Type(), err, proxy, f.testUrl, f.expectedStatus, callbacks)
	}

	if needHandshake {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				f.onDialSuccess()
			} else {
				f.onDialFailedWithCallbacks(ctx, proxy.Type(), err, proxy, f.testUrl, f.expectedStatus, callbacks)
			}
		})
	}
	if err == nil {
		c = f.observePostConnectFailureWithCallbacks(ctx, c, proxy.Type(), proxy, f.testUrl, f.expectedStatus, needHandshake, callbacks)
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

func (f *Fallback) CountRequest(metadata *C.Metadata) {
	proxy, selection := f.findAliveProxyWithSelection(true)
	f.onRequestAttempt(proxy, f.testUrl, f.expectedStatus, func() {
		f.healthCheckForProxy(proxy, selection)
	})
}

func (f *Fallback) healthCheck() {
	proxy, selection := f.findAliveProxyWithSelection(false)
	f.healthCheckForProxy(proxy, selection)
}

func (f *Fallback) healthCheckForProxy(proxy C.Proxy, selection manualSelectionSnapshot) {
	if proxy == nil {
		log.Warnln("[应用] fallback 范围健康检测\tgroup=%s\tproxy=<nil>\tscope=仅自身", f.Name())
		f.clearManualSelectionIfUnchanged(selection)
		f.GroupBase.healthCheck(f.testUrl, f.expectedStatus)
		closed := statistic.DefaultManager.CloseConnectionsUsingProxyGroup(f.Name())
		log.Warnln("[应用] fallback 范围关闭连接\tgroup=%s\tproxy=<nil>\tclosed=%d", f.Name(), closed)
		return
	}

	proxyName := proxy.Name()
	targets := f.fallbackGroupsUsingProxy(proxyName, selection)
	groupNames := make([]string, 0, len(targets))
	groupNamesLog := make([]string, 0, len(targets))
	for _, target := range targets {
		target.group.clearManualSelectionIfUnchanged(target.selection)
		target.group.GroupBase.healthCheck(target.group.testUrl, target.group.expectedStatus)
		groupNames = append(groupNames, target.group.Name())
		groupNamesLog = append(groupNamesLog, target.group.Name())
	}
	log.Warnln("[应用] fallback 范围健康检测\ttriggerGroup=%s\tproxy=%s\taffectedGroups=%s", f.Name(), proxyName, strings.Join(groupNamesLog, ","))
	closed := statistic.DefaultManager.CloseConnectionsUsingProxyGroupsAndProxy(groupNames, proxyName)
	log.Warnln("[应用] fallback 范围关闭连接\ttriggerGroup=%s\tproxy=%s\taffectedGroups=%s\tclosed=%d", f.Name(), proxyName, strings.Join(groupNamesLog, ","), closed)
}

type fallbackHealthCheckTarget struct {
	group     *Fallback
	selection manualSelectionSnapshot
}

func (f *Fallback) fallbackGroupsUsingProxy(proxyName string, selection manualSelectionSnapshot) []fallbackHealthCheckTarget {
	groups := []fallbackHealthCheckTarget{{group: f, selection: selection}}
	seen := map[*Fallback]struct{}{f: {}}
	for _, proxy := range tunnel.Proxies() {
		group, ok := proxy.Adapter().(*Fallback)
		if !ok {
			continue
		}
		if _, ok := seen[group]; ok {
			continue
		}
		current, currentSelection := group.findAliveProxyWithSelection(false)
		if current == nil || current.Name() != proxyName {
			continue
		}
		seen[group] = struct{}{}
		groups = append(groups, fallbackHealthCheckTarget{group: group, selection: currentSelection})
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
		"expectedStatus":  f.expectedStatus,
		"fixed":           f.selection.snapshot().name,
		"selectedTimeout": f.selectedTimeout,
		"hidden":          f.Hidden(),
		"icon":            f.Icon(),
		"connectTimes":    f.ConnectTimes(),
		"maxConnectTimes": f.MaxConnectTimes(),
	})
}

func (f *Fallback) DelayTestSpec() (string, string) {
	return f.testUrl, f.expectedStatus
}

// Unwrap implements C.ProxyAdapter
func (f *Fallback) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := f.findAliveProxy(touch)
	return proxy
}

func (f *Fallback) findAliveProxy(touch bool) C.Proxy {
	proxy, _ := f.findAliveProxyWithSelection(touch)
	return proxy
}

func (f *Fallback) findAliveProxyWithSelection(touch bool) (C.Proxy, manualSelectionSnapshot) {
	proxies := f.GetProxies(touch)
	timeoutMs := f.TestTimeout
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	// 手动选择模式：节点仍可用时固定使用。max-failed-times 健康检测会清钉。
	selection := f.selection.snapshot()
	if selection.name != "" {
		for _, proxy := range proxies {
			if proxy.Name() == selection.name {
				return proxy, selection // 直接返回，不检查延迟或健康状态
			}
		}
		// 选择的节点不存在于列表中（可能被移除），清空选择
		f.clearManualSelectionIfUnchanged(selection)
		selection = manualSelectionSnapshot{}
	}

	// 自动模式：返回第一个可用的节点
	for _, proxy := range proxies {
		// Only use proxy if alive and delay is within group timeout
		if proxy.AliveForTestUrl(f.testUrl) && proxy.LastDelayForTestUrl(f.testUrl) <= uint16(timeoutMs) {
			return proxy, selection
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
		return best, selection
	}
	return proxies[0], selection
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

	// 立即切换到用户选择的节点并固定使用。拨号失败本身不换节点；max-failed-times 健康检测会清钉。
	f.selection.set(name)

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
		ctx = C.WithHealthCheckSourceName(ctx, f.Name())
		expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
		_, _ = p.URLTest(ctx, f.testUrl, expectedStatus)
	}()

	return nil
}

func (f *Fallback) ForceSet(name string) {
	f.selection.set(name)
}

// NowIsManual implements NowIsManualAble.
func (f *Fallback) NowIsManual() bool {
	return f.selection.snapshot().name != ""
}

// ClearManualSelection clears the fixed selected node so the group auto-picks first alive.
func (f *Fallback) ClearManualSelection() {
	f.selection.clear()
}

func (f *Fallback) clearManualSelectionIfUnchanged(selection manualSelectionSnapshot) {
	if f.selection.clearIfUnchanged(selection) {
		f.onManualSelectionCleared()
	}
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
