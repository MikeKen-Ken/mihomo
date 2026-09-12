package adapter

import (
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/queue"
	C "github.com/metacubex/mihomo/constant"
	"time"
)

// RecordProbeHealth publishes only a primary success or independently confirmed
// failure. Diagnostic endpoint failures alone must never change this state.
func (p *Proxy) RecordProbeHealth(url string, delay uint16, healthy bool) {
	state, _ := p.extra.LoadOrStoreFn(url, func() *internalProxyState {
		return &internalProxyState{history: queue.New[C.DelayHistory](defaultHistoriesNum), alive: atomic.NewBool(true)}
	})
	state.alive.Store(healthy)
	if !healthy {
		delay = 0
	}
	state.history.Put(C.DelayHistory{Time: time.Now(), Delay: delay})
	if state.history.Len() > defaultHistoriesNum {
		state.history.Pop()
	}
}
