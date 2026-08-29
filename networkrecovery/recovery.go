package networkrecovery

import (
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

type Kind string

const (
	KindDNSChanged   Kind = "dns-changed"
	KindDNSFailure   Kind = "dns-failure"
	KindRouteChanged Kind = "route-changed"
	KindEscalated    Kind = "escalated"
)

const (
	dnsEscalationWindow  = 60 * time.Second
	fullRecoveryDebounce = time.Second
)

type Request struct {
	Kind   Kind   `json:"kind"`
	Reason string `json:"reason,omitempty"`
}

type Report struct {
	Sequence           uint64 `json:"sequence"`
	Kind               Kind   `json:"kind"`
	Action             string `json:"action"`
	Coalesced          bool   `json:"coalesced"`
	ClosedConnections  bool   `json:"closedConnections"`
	ResetAdapters      int    `json:"resetAdapters"`
	RestartRecommended bool   `json:"restartRecommended"`
	Error              string `json:"error,omitempty"`
}

type recoveryActions interface {
	resetDNS()
	resetRoute() (int, error)
}

type coreActions struct{}

func (coreActions) resetDNS() {
	resolver.ClearCache()
	resolver.ResetConnection()
}

func (coreActions) resetRoute() (int, error) {
	tunnel.CloseAllConnections()
	err := resolver.FlushFakeIP()
	return tunnel.ResetNetworkState(), err
}

type Manager struct {
	mu                    sync.Mutex
	actions               recoveryActions
	now                   func() time.Time
	lastDNSFailureAt      time.Time
	lastDNSFullRecoveryAt time.Time
	lastFullRecoveryAt    time.Time
	sequence              uint64
	lastReport            Report
}

func newManager(actions recoveryActions, now func() time.Time) *Manager {
	return &Manager{actions: actions, now: now}
}

var defaultManager = newManager(coreActions{}, time.Now)

func Recover(request Request) Report {
	return defaultManager.Recover(request)
}

func MarkHealthy() {
	defaultManager.MarkHealthy()
}

func Status() Report {
	return defaultManager.Status()
}

func (m *Manager) MarkHealthy() {
	m.mu.Lock()
	m.lastDNSFailureAt = time.Time{}
	m.lastDNSFullRecoveryAt = time.Time{}
	if m.lastReport.RestartRecommended {
		m.sequence++
		m.lastReport = Report{
			Sequence: m.sequence,
			Kind:     KindDNSFailure,
			Action:   "healthy",
		}
	}
	m.mu.Unlock()
}

func (m *Manager) Status() Report {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastReport
}

func (m *Manager) Recover(request Request) Report {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	report := Report{Kind: request.Kind}
	switch request.Kind {
	case KindDNSChanged:
		m.lastDNSFailureAt = time.Time{}
		m.lastDNSFullRecoveryAt = time.Time{}
		m.actions.resetDNS()
		report.Action = "dns-reset"
	case KindDNSFailure:
		if !m.lastDNSFailureAt.IsZero() && now.Sub(m.lastDNSFailureAt) <= dnsEscalationWindow {
			restartRecommended := !m.lastDNSFullRecoveryAt.IsZero() &&
				now.Sub(m.lastDNSFullRecoveryAt) <= dnsEscalationWindow
			report = m.fullRecovery(now, request, restartRecommended)
			m.lastDNSFullRecoveryAt = now
		} else {
			m.actions.resetDNS()
			report.Action = "dns-reset"
			m.lastDNSFullRecoveryAt = time.Time{}
		}
		m.lastDNSFailureAt = now
	case KindRouteChanged:
		m.lastDNSFailureAt = time.Time{}
		m.lastDNSFullRecoveryAt = time.Time{}
		report = m.fullRecovery(now, request, false)
	case KindEscalated:
		report = m.fullRecovery(now, request, true)
	default:
		report.Action = "ignored"
		report.Error = "unsupported recovery kind"
	}
	m.sequence++
	report.Sequence = m.sequence
	m.lastReport = report

	log.Infoln(
		"[Network] recovery kind=%s action=%s reason=%s coalesced=%t reset-adapters=%d",
		request.Kind,
		report.Action,
		request.Reason,
		report.Coalesced,
		report.ResetAdapters,
	)
	return report
}

func (m *Manager) fullRecovery(now time.Time, request Request, restartRecommended bool) Report {
	report := Report{
		Kind:               request.Kind,
		Action:             "route-reset",
		ClosedConnections:  true,
		RestartRecommended: restartRecommended,
	}
	if !m.lastFullRecoveryAt.IsZero() && now.Sub(m.lastFullRecoveryAt) < fullRecoveryDebounce {
		report.Action = "coalesced"
		report.Coalesced = true
		report.ClosedConnections = false
		return report
	}

	m.lastFullRecoveryAt = now
	reset, err := m.actions.resetRoute()
	report.ResetAdapters = reset
	if err != nil {
		report.Error = err.Error()
	}
	return report
}
