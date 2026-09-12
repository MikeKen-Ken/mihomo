package outboundgroup

import (
	C "github.com/metacubex/mihomo/constant"
	"sync"
	"time"
)

type fallbackStability struct {
	mu    sync.Mutex
	name  string
	until time.Time
}

func (s *fallbackStability) current(proxies []C.Proxy, url string, timeout int) C.Proxy {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Now().After(s.until) {
		return nil
	}
	for _, proxy := range proxies {
		if proxy.Name() == s.name && proxy.AliveForTestUrl(url) && int(proxy.LastDelayForTestUrl(url)) <= timeout {
			return proxy
		}
	}
	return nil
}

func (s *fallbackStability) remember(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name != s.name {
		s.name = name
		s.until = time.Now().Add(2 * time.Minute)
	}
}
