package provider

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/dlclark/regexp2"
	"golang.org/x/sync/errgroup"
)

type HealthCheckOption struct {
	URL      string
	Interval uint
}

type extraOption struct {
	expectedStatus utils.IntRanges[uint16]
	filters        map[string]struct{}
}

type HealthCheck struct {
	ctx            context.Context
	ctxCancel      context.CancelFunc
	name           string
	url            string
	extra          map[string]*extraOption
	mu             sync.Mutex
	callbacks      []func()
	proxies        []C.Proxy
	interval       time.Duration
	lazy           bool
	expectedStatus utils.IntRanges[uint16]
	lastTouch      atomic.TypedValue[time.Time]
	singleDo       *singledo.Single[struct{}]
	timeout        time.Duration
}

func (hc *HealthCheck) process() {
	ticker := time.NewTicker(hc.interval)
	go hc.check()
	for {
		select {
		case <-ticker.C:
			lastTouch := hc.lastTouch.Load()
			since := time.Since(lastTouch)
			if !hc.lazy || since < hc.interval {
				hc.check()
			} else {
				log.Infoln("[%s] 跳过本次健康检测（lazy 模式）", hc.name)
			}
		case <-hc.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (hc *HealthCheck) setProxies(proxies []C.Proxy) {
	hc.proxies = proxies
}

func (hc *HealthCheck) registerHealthCheckTask(url string, expectedStatus utils.IntRanges[uint16], filter string, interval uint) {
	url = strings.TrimSpace(url)
	if len(url) == 0 || url == hc.url {
		log.Infoln("[%s] 忽略无效健康检测 URL: %s", hc.name, url)
		return
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// if the provider has not set up health checks, then modify it to be the same as the group's interval
	if hc.interval == 0 {
		hc.interval = time.Duration(interval) * time.Second
	}

	if hc.extra == nil {
		hc.extra = make(map[string]*extraOption)
	}

	// prioritize the use of previously registered configurations, especially those from provider
	if _, ok := hc.extra[url]; ok {
		// provider default health check does not set filter
		if url != hc.url && len(filter) != 0 {
			splitAndAddFiltersToExtra(filter, hc.extra[url])
		}

		log.Infoln("[%s] 健康检测 URL 已存在: %s", hc.name, url)
		return
	}

	option := &extraOption{filters: map[string]struct{}{}, expectedStatus: expectedStatus}
	splitAndAddFiltersToExtra(filter, option)
	hc.extra[url] = option
}

func (hc *HealthCheck) registerHealthCheckCallback(callback func()) {
	if callback == nil {
		return
	}

	hc.mu.Lock()
	hc.callbacks = append(hc.callbacks, callback)
	hc.mu.Unlock()
}

func (hc *HealthCheck) notifyHealthCheckCallbacks() {
	hc.mu.Lock()
	callbacks := append([]func(){}, hc.callbacks...)
	hc.mu.Unlock()

	for _, callback := range callbacks {
		callback()
	}
}

func splitAndAddFiltersToExtra(filter string, option *extraOption) {
	filter = strings.TrimSpace(filter)
	if len(filter) != 0 {
		for _, regex := range strings.Split(filter, "`") {
			regex = strings.TrimSpace(regex)
			if len(regex) != 0 {
				option.filters[regex] = struct{}{}
			}
		}
	}
}

func (hc *HealthCheck) auto() bool {
	return hc.interval != 0
}

func (hc *HealthCheck) touch() {
	hc.lastTouch.Store(time.Now())
}

func (hc *HealthCheck) check() {
	hc.checkAll()
}

func (hc *HealthCheck) checkURLUntilHealthy(url string, expectedStatus utils.IntRanges[uint16], targetNames map[string]struct{}) bool {
	if len(hc.proxies) == 0 {
		return false
	}
	defer hc.notifyHealthCheckCallbacks()

	id := utils.NewUUIDV4().String()
	log.Infoln("[%s] 开始健康检测（持续至健康）{%s}", hc.name, id)
	option := hc.optionForURL(url, expectedStatus)
	foundHealthy := hc.execute(url, id, option, true, targetNames)
	log.Infoln("[%s] 健康检测完成（持续至健康）{%s}", hc.name, id)
	return foundHealthy
}

func (hc *HealthCheck) checkAll() {
	if len(hc.proxies) == 0 {
		return
	}
	defer hc.notifyHealthCheckCallbacks()

	_, _, _ = hc.singleDo.Do(func() (struct{}, error) {
		id := utils.NewUUIDV4().String()
		log.Infoln("[%s] 开始健康检测 {%s}", hc.name, id)

		option := &extraOption{filters: nil, expectedStatus: hc.expectedStatus}
		hc.execute(hc.url, id, option, false, nil)

		if len(hc.extra) != 0 {
			for url, option := range hc.extra {
				hc.execute(url, id, option, false, nil)
			}
		}
		log.Infoln("[%s] 健康检测完成 {%s}", hc.name, id)
		return struct{}{}, nil
	})
}

func (hc *HealthCheck) optionForURL(url string, expectedStatus utils.IntRanges[uint16]) *extraOption {
	url = strings.TrimSpace(url)
	if url == hc.url {
		return &extraOption{filters: nil, expectedStatus: hc.expectedStatus}
	}

	hc.mu.Lock()
	option, ok := hc.extra[url]
	if ok {
		option = cloneExtraOption(option)
	}
	hc.mu.Unlock()
	if ok {
		return option
	}

	return &extraOption{filters: nil, expectedStatus: expectedStatus}
}

func cloneExtraOption(option *extraOption) *extraOption {
	if option == nil {
		return nil
	}

	clone := &extraOption{expectedStatus: option.expectedStatus}
	if len(option.filters) != 0 {
		clone.filters = make(map[string]struct{}, len(option.filters))
		for filter := range option.filters {
			clone.filters[filter] = struct{}{}
		}
	}
	return clone
}

func (hc *HealthCheck) execute(url, uid string, option *extraOption, stopOnFirstHealthy bool, targetNames map[string]struct{}) bool {
	url = strings.TrimSpace(url)
	if len(url) == 0 {
		log.Infoln("[%s] 健康检测跳过，testUrl 为空 {%s}", hc.name, uid)
		return false
	}

	var filterReg *regexp2.Regexp
	var expectedStatus utils.IntRanges[uint16]
	if option != nil {
		expectedStatus = option.expectedStatus
		if len(option.filters) != 0 {
			filters := make([]string, 0, len(option.filters))
			for filter := range option.filters {
				filters = append(filters, filter)
			}

			filterReg = regexp2.MustCompile(strings.Join(filters, "|"), regexp2.None)
		}
	}

	targets := make([]C.Proxy, 0, len(hc.proxies))
	for _, proxy := range hc.proxies {
		if targetNames != nil {
			if _, ok := targetNames[proxy.Name()]; !ok {
				continue
			}
		}

		// skip proxies that do not require health check
		if filterReg != nil {
			if match, _ := filterReg.MatchString(proxy.Name()); !match {
				continue
			}
		}
		targets = append(targets, proxy)
	}

	if len(targets) == 0 {
		return false
	}

	workerLimit := EffectiveHealthCheckWorkerLimit()
	if workerLimit > len(targets) {
		workerLimit = len(targets)
	}

	anyHealthy := false
	for start := 0; start < len(targets); start += workerLimit {
		end := start + workerLimit
		if end > len(targets) {
			end = len(targets)
		}
		batch := targets[start:end]
		foundHealthy := false
		healthyMux := sync.Mutex{}
		b := new(errgroup.Group)

		for _, proxy := range batch {
			p := proxy
			b.Go(func() error {
				base := C.WithHealthCheckSourceName(hc.ctx, hc.name)
				ctx, cancel := context.WithTimeout(base, hc.timeout)
				defer cancel()
				_, err := p.URLTest(ctx, url, expectedStatus)
				alive := err == nil && p.AliveForTestUrl(url)
				if alive {
					healthyMux.Lock()
					foundHealthy = true
					healthyMux.Unlock()
				}
				return nil
			})
		}

		_ = b.Wait()
		if foundHealthy && stopOnFirstHealthy {
			log.Infoln("[%s] 健康检测批次命中可用节点，停止后续批次，url: %s, id: {%s}", hc.name, url, uid)
			return true
		}
		if foundHealthy {
			anyHealthy = true
		}
	}
	return anyHealthy
}

func (hc *HealthCheck) close() {
	hc.ctxCancel()
}

func NewHealthCheck(proxies []C.Proxy, name string, url string, timeout uint, interval uint, lazy bool, expectedStatus utils.IntRanges[uint16]) *HealthCheck {
	if url == "" {
		expectedStatus = nil
		interval = 0
	}
	if timeout == 0 {
		timeout = 5000
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &HealthCheck{
		ctx:            ctx,
		ctxCancel:      cancel,
		name:           name,
		proxies:        proxies,
		url:            url,
		timeout:        time.Duration(timeout) * time.Millisecond,
		extra:          map[string]*extraOption{},
		interval:       time.Duration(interval) * time.Second,
		lazy:           lazy,
		expectedStatus: expectedStatus,
		singleDo:       singledo.NewSingle[struct{}](time.Second),
	}
}
