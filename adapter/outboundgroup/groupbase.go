package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/dlclark/regexp2"
	"golang.org/x/exp/slices"
)

const (
	maxConnectTimesTestEventPrefix = "max-connect-times\t"
	proxyGroupRefreshEventPrefix   = "proxy-group-refresh\t"
)

const (
	maxConnectTimesTestCooldown = 30 * time.Second
)

type GroupBase struct {
	*outbound.Base
	hidden            bool
	icon              string
	filterRegs        []*regexp2.Regexp
	excludeFilterRegs []*regexp2.Regexp
	excludeTypeArray  []string
	providers         []P.ProxyProvider
	failedTestMux        sync.Mutex
	failedTimes          int
	failedTime           time.Time
	failedTesting        atomic.Bool
	connectTestMux       sync.Mutex
	connectTimes         int
	lastConnectTestAt    time.Time
	connectTesting       atomic.Bool
	TestTimeout          int
	failureResetInterval int
	maxFailedTimes       int
	maxConnectTimes      int

	// for GetProxies
	getProxiesMutex  sync.Mutex
	providerVersions []uint32
	providerProxies  []C.Proxy
}

type GroupBaseOption struct {
	Name                  string
	Type                  C.AdapterType
	Hidden                bool
	Icon                  string
	Filter                string
	ExcludeFilter         string
	ExcludeType           string
	TestTimeout           int
	FailureResetInterval  int
	MaxFailedTimes        int
	MaxConnectTimes       int
	Providers             []P.ProxyProvider
}

func NewGroupBase(opt GroupBaseOption) *GroupBase {
	var excludeTypeArray []string
	if opt.ExcludeType != "" {
		excludeTypeArray = strings.Split(opt.ExcludeType, "|")
	}

	var excludeFilterRegs []*regexp2.Regexp
	if opt.ExcludeFilter != "" {
		for _, excludeFilter := range strings.Split(opt.ExcludeFilter, "`") {
			excludeFilterReg := regexp2.MustCompile(excludeFilter, regexp2.None)
			excludeFilterRegs = append(excludeFilterRegs, excludeFilterReg)
		}
	}

	var filterRegs []*regexp2.Regexp
	if opt.Filter != "" {
		for _, filter := range strings.Split(opt.Filter, "`") {
			filterReg := regexp2.MustCompile(filter, regexp2.None)
			filterRegs = append(filterRegs, filterReg)
		}
	}

	gb := &GroupBase{
		Base:              outbound.NewBase(outbound.BaseOption{Name: opt.Name, Type: opt.Type}),
		hidden:            opt.Hidden,
		icon:              opt.Icon,
		filterRegs:        filterRegs,
		excludeFilterRegs: excludeFilterRegs,
		excludeTypeArray:  excludeTypeArray,
		providers:         opt.Providers,
		failedTesting:     atomic.NewBool(false),
		connectTesting:    atomic.NewBool(false),
		TestTimeout:          opt.TestTimeout,
		failureResetInterval: opt.FailureResetInterval,
		maxFailedTimes:        opt.MaxFailedTimes,
		maxConnectTimes:       opt.MaxConnectTimes,
	}

	if gb.TestTimeout == 0 {
		gb.TestTimeout = 5000
	}
	if gb.failureResetInterval == 0 {
		gb.failureResetInterval = 5000
	}
	if gb.maxFailedTimes == 0 {
		gb.maxFailedTimes = 5
	}

	return gb
}

func (gb *GroupBase) Hidden() bool {
	return gb.hidden
}

func (gb *GroupBase) Icon() string {
	return gb.icon
}

func (gb *GroupBase) Touch() {
	for _, pd := range gb.providers {
		pd.Touch()
	}
}

func (gb *GroupBase) GetProxies(touch bool) []C.Proxy {
	providerVersions := make([]uint32, len(gb.providers))
	for i, pd := range gb.providers {
		if touch { // touch first
			pd.Touch()
		}
		providerVersions[i] = pd.Version()
	}

	// thread safe
	gb.getProxiesMutex.Lock()
	defer gb.getProxiesMutex.Unlock()

	// return the cached proxies if version not changed
	if slices.Equal(providerVersions, gb.providerVersions) {
		return gb.providerProxies
	}

	var proxies []C.Proxy
	if len(gb.filterRegs) == 0 {
		for _, pd := range gb.providers {
			proxies = append(proxies, pd.Proxies()...)
		}
	} else {
		for _, pd := range gb.providers {
			if pd.VehicleType() == P.Compatible { // compatible provider unneeded filter
				proxies = append(proxies, pd.Proxies()...)
				continue
			}

			var newProxies []C.Proxy
			proxiesSet := map[string]struct{}{}
			for _, filterReg := range gb.filterRegs {
				for _, p := range pd.Proxies() {
					name := p.Name()
					if mat, _ := filterReg.MatchString(name); mat {
						if _, ok := proxiesSet[name]; !ok {
							proxiesSet[name] = struct{}{}
							newProxies = append(newProxies, p)
						}
					}
				}
			}
			proxies = append(proxies, newProxies...)
		}
	}

	// Multiple filers means that proxies are sorted in the order in which the filers appear.
	// Although the filter has been performed once in the previous process,
	// when there are multiple providers, the array needs to be reordered as a whole.
	if len(gb.providers) > 1 && len(gb.filterRegs) > 1 {
		var newProxies []C.Proxy
		proxiesSet := map[string]struct{}{}
		for _, filterReg := range gb.filterRegs {
			for _, p := range proxies {
				name := p.Name()
				if mat, _ := filterReg.MatchString(name); mat {
					if _, ok := proxiesSet[name]; !ok {
						proxiesSet[name] = struct{}{}
						newProxies = append(newProxies, p)
					}
				}
			}
		}
		for _, p := range proxies { // add not matched proxies at the end
			name := p.Name()
			if _, ok := proxiesSet[name]; !ok {
				proxiesSet[name] = struct{}{}
				newProxies = append(newProxies, p)
			}
		}
		proxies = newProxies
	}

	if len(gb.excludeFilterRegs) > 0 {
		var newProxies []C.Proxy
	LOOP1:
		for _, p := range proxies {
			name := p.Name()
			for _, excludeFilterReg := range gb.excludeFilterRegs {
				if mat, _ := excludeFilterReg.MatchString(name); mat {
					continue LOOP1
				}
			}
			newProxies = append(newProxies, p)
		}
		proxies = newProxies
	}

	if gb.excludeTypeArray != nil {
		var newProxies []C.Proxy
	LOOP2:
		for _, p := range proxies {
			mType := p.Type().String()
			for _, excludeType := range gb.excludeTypeArray {
				if strings.EqualFold(mType, excludeType) {
					continue LOOP2
				}
			}
			newProxies = append(newProxies, p)
		}
		proxies = newProxies
	}

	if len(proxies) == 0 {
		return []C.Proxy{tunnel.Proxies()["COMPATIBLE"]}
	}

	// only cache when proxies not empty
	gb.providerVersions = providerVersions
	gb.providerProxies = proxies

	return proxies
}

func (gb *GroupBase) onRequestAttempt(proxy C.Proxy, testURL string, expectedStatus string, fn func()) {
	if gb.maxConnectTimes <= 0 || proxy == nil || testURL == "" {
		return
	}
	// 健康检查期间不计 max-connect-times 请求计数，避免与真实流量叠加误触发
	if gb.failedTesting.Load() {
		return
	}
	// max-connect-times 内置 URL 测速期间不计数，避免测速连接抬高计数
	if gb.connectTesting.Load() {
		return
	}

	adapterType := proxy.Type()
	if adapterType == C.Direct || adapterType == C.Compatible || adapterType == C.Reject || adapterType == C.Pass || adapterType == C.RejectDrop {
		return
	}

	shouldTest := false
	currentConnectTimes := 0
	gb.connectTestMux.Lock()
	gb.connectTimes++
	currentConnectTimes = gb.connectTimes
	if gb.connectTimes >= gb.maxConnectTimes {
		gb.connectTimes = 0
		if gb.lastConnectTestAt.IsZero() || time.Since(gb.lastConnectTestAt) >= maxConnectTimesTestCooldown {
			shouldTest = true
		}
	}
	gb.connectTestMux.Unlock()
	notifyProxyGroupRefresh(gb.Name())

	if !shouldTest {
		log.Debugln("ProxyGroup: %s max-connect-times request count not reached cooldown, count=%d/%d", gb.Name(), currentConnectTimes, gb.maxConnectTimes)
		return
	}
	if !gb.connectTesting.CompareAndSwap(false, true) {
		return
	}
	gb.connectTestMux.Lock()
	gb.lastConnectTestAt = time.Now()
	gb.connectTestMux.Unlock()

	go func() {
		defer gb.connectTesting.Store(false)

		timeoutMs := gb.TestTimeout
		if timeoutMs <= 0 {
			timeoutMs = 5000
		}

		status, err := utils.NewUnsignedRanges[uint16](expectedStatus)
		if err != nil {
			log.Debugln("ProxyGroup: %s max connect times test skipped: %v", gb.Name(), err)
			return
		}

		runURLTest := func() (uint16, error) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(timeoutMs))
			defer cancel()
			ctx = C.WithHealthCheckSourceName(ctx, gb.Name())

			return proxy.URLTest(ctx, testURL, status)
		}

		log.Warnln("[APP] max-connect-times test triggered\tgroup=%s\tproxy=%s\tthreshold=%d\ttimeoutMs=%d", gb.Name(), proxy.Name(), gb.maxConnectTimes, timeoutMs)
		notifyMaxConnectTimesTestTriggered(gb.Name(), proxy.Name())

		delay, testErr := runURLTest()
		if testErr == nil {
			log.Warnln("[APP] max-connect-times test result\t%s\t%s\tsuccess\t%d", gb.Name(), proxy.Name(), delay)
			return
		}

		log.Warnln("[APP] max-connect-times test result\t%s\t%s\tfail\t%v", gb.Name(), proxy.Name(), testErr)
		log.Debugln("ProxyGroup: %s current proxy %s failed max connect times test: %v", gb.Name(), proxy.Name(), testErr)
		log.Warnln("[APP] max-connect-times test retry\tgroup=%s\tproxy=%s\treason=first-test-failed", gb.Name(), proxy.Name())

		retryDelay, retryErr := runURLTest()
		if retryErr == nil {
			log.Warnln("[APP] max-connect-times test result\t%s\t%s\tsuccess\t%d", gb.Name(), proxy.Name(), retryDelay)
			return
		}

		log.Warnln("[APP] max-connect-times test result\t%s\t%s\tfail\t%v", gb.Name(), proxy.Name(), retryErr)
		log.Warnln("[APP] max-connect-times health-check triggered\tgroup=%s\tproxy=%s\treason=retry-fail", gb.Name(), proxy.Name())
		log.Infoln("ProxyGroup: %s current proxy %s failed max connect times test twice, trigger health check", gb.Name(), proxy.Name())
		fn()
	}()
}

func (gb *GroupBase) resetConnectTimes() {
	gb.connectTestMux.Lock()
	gb.connectTimes = 0
	gb.lastConnectTestAt = time.Time{}
	gb.connectTestMux.Unlock()
	notifyProxyGroupRefresh(gb.Name())
}

func (gb *GroupBase) ResetConnectTimes() {
	gb.resetConnectTimes()
}

func (gb *GroupBase) ConnectTimes() int {
	gb.connectTestMux.Lock()
	defer gb.connectTestMux.Unlock()
	return gb.connectTimes
}

func (gb *GroupBase) MaxConnectTimes() int {
	return gb.maxConnectTimes
}

func (gb *GroupBase) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	defer gb.resetConnectTimes()

	testCtx := C.WithHealthCheckSourceName(ctx, gb.Name())

	var wg sync.WaitGroup
	var lock sync.Mutex
	mp := map[string]uint16{}
	proxies := gb.GetProxies(false)
	for _, proxy := range proxies {
		proxy := proxy
		wg.Add(1)
		go func() {
			delay, err := proxy.URLTest(testCtx, url, expectedStatus)
			if err == nil {
				lock.Lock()
				mp[proxy.Name()] = delay
				lock.Unlock()
			}

			wg.Done()
		}()
	}
	wg.Wait()

	if len(mp) == 0 {
		return mp, fmt.Errorf("get delay: all proxies timeout")
	} else {
		return mp, nil
	}
}

// shouldSuppressDialFailureStats 测速（URLTest 标记的 ctx）或本组健康检查期间不计入 max-failed-times。
func (gb *GroupBase) shouldSuppressDialFailureStats(ctx context.Context) bool {
	if gb.failedTesting.Load() {
		return true
	}
	return C.SuppressGroupOutboundFailureStats(ctx)
}

func (gb *GroupBase) onDialFailed(ctx context.Context, adapterType C.AdapterType, err error, fn func()) {
	if adapterType == C.Direct || adapterType == C.Compatible || adapterType == C.Reject || adapterType == C.Pass || adapterType == C.RejectDrop {
		return
	}

	if errors.Is(err, C.ErrNotSupport) {
		return
	}

	go func() {
		if gb.shouldSuppressDialFailureStats(ctx) {
			return
		}
		if strings.Contains(err.Error(), "connection refused") {
			log.Warnln("[APP] max-failed-times health-check triggered\tgroup=%s\treason=connection-refused", gb.Name())
			fn()
			return
		}

		gb.failedTestMux.Lock()
		defer gb.failedTestMux.Unlock()

		gb.failedTimes++
		if gb.failedTimes == 1 {
			log.Debugln("ProxyGroup: %s first failed", gb.Name())
			gb.failedTime = time.Now()
			log.Warnln("[APP] max-failed-times updated\tgroup=%s\tcount=%d\tthreshold=%d\twindowMs=%d", gb.Name(), gb.failedTimes, gb.maxFailedTimes, gb.failureResetInterval)
			if gb.failedTimes >= gb.maxFailedTimes {
				log.Warnln("because %s failed multiple times, active health check", gb.Name())
				log.Warnln("[APP] max-failed-times health-check triggered\tgroup=%s\tcount=%d\tthreshold=%d", gb.Name(), gb.failedTimes, gb.maxFailedTimes)
				fn()
			}
		} else {
			if time.Since(gb.failedTime) > time.Duration(gb.failureResetInterval)*time.Millisecond {
				log.Warnln("[APP] max-failed-times reset\tgroup=%s\treason=window-expired\tcount=%d", gb.Name(), gb.failedTimes)
				gb.failedTimes = 0
				return
			}

			log.Debugln("ProxyGroup: %s failed count: %d", gb.Name(), gb.failedTimes)
			log.Warnln("[APP] max-failed-times updated\tgroup=%s\tcount=%d\tthreshold=%d\twindowMs=%d", gb.Name(), gb.failedTimes, gb.maxFailedTimes, gb.failureResetInterval)
			if gb.failedTimes >= gb.maxFailedTimes {
				log.Warnln("because %s failed multiple times, activate health check", gb.Name())
				log.Warnln("[APP] max-failed-times health-check triggered\tgroup=%s\tcount=%d\tthreshold=%d", gb.Name(), gb.failedTimes, gb.maxFailedTimes)
				fn()
			}
		}
	}()
}

func (gb *GroupBase) healthCheck(testURL string, expectedStatusText string) {
	if !gb.failedTesting.CompareAndSwap(false, true) {
		return
	}
	defer func() {
		gb.failedTesting.Store(false)
	}()

	// Notify health check triggered for fallback groups (CFA only)
	if gb.Type() == C.Fallback {
		log.Infoln("Fallback group %s triggered health check", gb.Name())
		notifyHealthCheckTriggered(gb.Name())
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](expectedStatusText)
	if err != nil {
		log.Warnln("ProxyGroup: %s parse expected status failed: %s", gb.Name(), err.Error())
		expectedStatus = nil
	}
	targetNames := gb.healthCheckTargetNames()

	for _, proxyProvider := range gb.providers {
		if testURL == "" {
			// testURL 为空时无法做早停判断，退化为全量串行检查
			proxyProvider.HealthCheck()
			continue
		}
		if proxyProvider.HealthCheckURLUntilHealthy(testURL, expectedStatus, targetNames) {
			log.Infoln("ProxyGroup: %s stop health check early after finding healthy proxy", gb.Name())
			break
		}
	}

	gb.failedTimes = 0
	gb.resetConnectTimes()
}

func (gb *GroupBase) healthCheckTargetNames() map[string]struct{} {
	proxies := gb.GetProxies(false)
	names := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		names[proxy.Name()] = struct{}{}
	}
	return names
}

func (gb *GroupBase) onDialSuccess() {
	if !gb.failedTesting.Load() {
		gb.failedTimes = 0
	}
}

type postConnectFailureConn struct {
	C.Conn
	callback       func(error)
	once           sync.Once
	writeMux       sync.Mutex
	written        bool
	skipFirstWrite bool
}

func (c *postConnectFailureConn) notify(err error) {
	if !isPostConnectFailure(err) {
		return
	}
	c.once.Do(func() {
		c.callback(err)
	})
}

func (c *postConnectFailureConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.notify(err)
	return n, err
}

func (c *postConnectFailureConn) ReadBuffer(buffer *buf.Buffer) error {
	err := c.Conn.ReadBuffer(buffer)
	c.notify(err)
	return err
}

func (c *postConnectFailureConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if c.shouldNotifyWrite() {
		c.notify(err)
	}
	return n, err
}

func (c *postConnectFailureConn) WriteBuffer(buffer *buf.Buffer) error {
	err := c.Conn.WriteBuffer(buffer)
	if c.shouldNotifyWrite() {
		c.notify(err)
	}
	return err
}

func (c *postConnectFailureConn) Upstream() any {
	return c.Conn
}

func (c *postConnectFailureConn) ReaderReplaceable() bool {
	return false
}

func (c *postConnectFailureConn) WriterReplaceable() bool {
	return false
}

func (c *postConnectFailureConn) shouldNotifyWrite() bool {
	c.writeMux.Lock()
	defer c.writeMux.Unlock()

	if !c.written {
		c.written = true
		return !c.skipFirstWrite
	}
	return true
}

func (gb *GroupBase) observePostConnectFailure(ctx context.Context, c C.Conn, adapterType C.AdapterType, skipFirstWrite bool, fn func()) C.Conn {
	return &postConnectFailureConn{
		Conn:           c,
		skipFirstWrite: skipFirstWrite,
		callback: func(err error) {
			gb.onDialFailed(ctx, adapterType, err, fn)
		},
	}
}

func isPostConnectFailure(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "forcibly closed by the remote host") ||
		strings.Contains(msg, "connection aborted")
}

var _ N.ExtendedConn = (*postConnectFailureConn)(nil)
