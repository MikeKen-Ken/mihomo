package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type Selector struct {
	*GroupBase
	disableUDP      bool
	selected        string
	manualSelected  bool // true only after user or persist Set(); cleared by ClearManualSelection/health/stop
	testUrl         string
	expectedStatus  string
	selectedTimeout int // ms, for manual-selected node delay test; 0 = skip
}

// DialContext implements C.ProxyAdapter
func (s *Selector) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	c, err := s.selectedProxy(true).DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(s)
	}
	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (s *Selector) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	pc, err := s.selectedProxy(true).ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(s)
	}
	return pc, err
}

func (s *Selector) CountRequest(metadata *C.Metadata) {
	s.onRequestAttempt(s.selectedProxy(false), s.testUrl, s.expectedStatus, func() {})
}

// SupportUDP implements C.ProxyAdapter
func (s *Selector) SupportUDP() bool {
	if s.disableUDP {
		return false
	}

	return s.selectedProxy(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (s *Selector) IsL3Protocol(metadata *C.Metadata) bool {
	return s.selectedProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (s *Selector) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range s.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	// When testurl is the default value
	// do not append a value to ensure that the web dashboard follows the settings of the dashboard
	var url string
	if s.testUrl != C.DefaultTestURL {
		url = s.testUrl
	}

	return json.Marshal(map[string]any{
		"type":            s.Type().String(),
		"now":             s.Now(),
		"all":             all,
		"testUrl":         url,
		"hidden":          s.Hidden(),
		"icon":            s.Icon(),
		"connectTimes":    s.ConnectTimes(),
		"maxConnectTimes": s.MaxConnectTimes(),
	})
}

func (s *Selector) Now() string {
	return s.selectedProxy(false).Name()
}

func (s *Selector) Set(name string) error {
	var selectedProxy C.Proxy
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() == name {
			selectedProxy = proxy
			s.selected = name
			s.manualSelected = true
			break
		}
	}
	if selectedProxy == nil {
		return errors.New("proxy not exist")
	}

	// 异步测速：手动选择的节点在后台测速，不阻塞返回（与安卓端体验对齐）
	if s.selectedTimeout > 0 && s.testUrl != "" {
		go func() {
			defer s.resetConnectTimes()

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(s.selectedTimeout))
			defer cancel()
			ctx = C.WithHealthCheckSourceName(ctx, s.Name())
			expectedStatus, _ := utils.NewUnsignedRanges[uint16](s.expectedStatus)
			_, _ = selectedProxy.URLTest(ctx, s.testUrl, expectedStatus)
		}()
	}

	return nil
}

func (s *Selector) ForceSet(name string) {
	s.selected = name
	s.manualSelected = name != ""
}

// NowIsManual implements NowIsManualAble.
func (s *Selector) NowIsManual() bool {
	return s.manualSelected
}

// ClearManualSelection clears manual flag without changing current selected node.
// 这样测速流程不会把当前节点强制切到列表第一个（可能是超时节点）。
func (s *Selector) ClearManualSelection() {
	s.manualSelected = false
}

// Unwrap implements C.ProxyAdapter
func (s *Selector) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return s.selectedProxy(touch)
}

func (s *Selector) selectedProxy(touch bool) C.Proxy {
	proxies := s.GetProxies(touch)
	for _, proxy := range proxies {
		if proxy.Name() == s.selected {
			return proxy
		}
	}

	return proxies[0]
}

func (s *Selector) Providers() []P.ProxyProvider {
	return s.providers
}

func (s *Selector) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func (s *Selector) DelayTestSpec() (string, string) {
	return s.testUrl, s.expectedStatus
}

func NewSelector(option *GroupCommonOption, providers []P.ProxyProvider) *Selector {
	selectedTimeout := option.SelectedTimeout
	if selectedTimeout <= 0 {
		selectedTimeout = option.TestTimeout
	}
	if selectedTimeout <= 0 {
		selectedTimeout = 5000
	}

	return &Selector{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:                 option.Name,
			Type:                 C.Selector,
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
		selected:        "COMPATIBLE",
		disableUDP:      option.DisableUDP,
		testUrl:         option.URL,
		expectedStatus:  option.ExpectedStatus,
		selectedTimeout: selectedTimeout,
	}
}
