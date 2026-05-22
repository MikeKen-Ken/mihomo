package statistic

import (
	"os"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/memory"
)

var DefaultManager *Manager

func init() {
	DefaultManager = &Manager{
		uploadTemp:    atomic.NewInt64(0),
		downloadTemp:  atomic.NewInt64(0),
		uploadBlip:    atomic.NewInt64(0),
		downloadBlip:  atomic.NewInt64(0),
		uploadTotal:   atomic.NewInt64(0),
		downloadTotal: atomic.NewInt64(0),
		pid:           int32(os.Getpid()),
	}

	go DefaultManager.handle()
}

type Manager struct {
	connections   xsync.Map[string, Tracker]
	uploadTemp    atomic.Int64
	downloadTemp  atomic.Int64
	uploadBlip    atomic.Int64
	downloadBlip  atomic.Int64
	uploadTotal   atomic.Int64
	downloadTotal atomic.Int64
	pid           int32
	memory        uint64
}

func (m *Manager) Join(c Tracker) {
	m.connections.Store(c.ID(), c)
}

func (m *Manager) Leave(c Tracker) {
	if info := c.Info(); info != nil {
		recordRecentClosedIfNeeded(info)
	}
	m.connections.Delete(c.ID())
}

func (m *Manager) Get(id string) (c Tracker) {
	if value, ok := m.connections.Load(id); ok {
		c = value
	}
	return
}

func (m *Manager) Range(f func(c Tracker) bool) {
	m.connections.Range(func(key string, value Tracker) bool {
		return f(value)
	})
}

// CloseConnectionsExcludingDirect closes all tracked connections whose chain does not contain DIRECT,
// so traffic is re-established with the new node (e.g. after fallback group health check).
func (m *Manager) CloseConnectionsExcludingDirect() {
	var toClose []string
	m.Range(func(c Tracker) bool {
		info := c.Info()
		if info == nil {
			return true
		}
		hasDirect := false
		for _, name := range info.Chain {
			if name == "DIRECT" {
				hasDirect = true
				break
			}
		}
		if !hasDirect {
			toClose = append(toClose, c.ID())
		}
		return true
	})
	for _, id := range toClose {
		if c := m.Get(id); c != nil {
			_ = c.Close()
		}
	}
}

// CloseConnectionsUsingProxyGroup 关闭链路中包含指定代理组名称的连接，
// 便于手动 PatchSelector 后流量尽快走新选中的节点。
func (m *Manager) CloseConnectionsUsingProxyGroup(group string) int {
	if group == "" {
		return 0
	}
	var toClose []string
	m.Range(func(c Tracker) bool {
		info := c.Info()
		if info == nil {
			return true
		}
		for _, name := range info.Chain {
			if name == group {
				toClose = append(toClose, c.ID())
				break
			}
		}
		return true
	})
	for _, id := range toClose {
		if c := m.Get(id); c != nil {
			_ = c.Close()
		}
	}
	return len(toClose)
}

// CloseConnectionsUsingProxyGroupsAndProxy 只关闭链路中同时包含指定代理组和节点的连接，
// 避免某个 fallback 组健康检测时误伤其它未使用该节点的连接。
func (m *Manager) CloseConnectionsUsingProxyGroupsAndProxy(groups []string, proxy string) int {
	if len(groups) == 0 || proxy == "" {
		return 0
	}

	groupSet := map[string]struct{}{}
	for _, group := range groups {
		if group != "" {
			groupSet[group] = struct{}{}
		}
	}
	if len(groupSet) == 0 {
		return 0
	}

	var toClose []string
	m.Range(func(c Tracker) bool {
		info := c.Info()
		if info == nil {
			return true
		}
		hasProxy := false
		hasGroup := false
		for _, name := range info.Chain {
			if name == proxy {
				hasProxy = true
			}
			if _, ok := groupSet[name]; ok {
				hasGroup = true
			}
			if hasProxy && hasGroup {
				toClose = append(toClose, c.ID())
				break
			}
		}
		return true
	})
	for _, id := range toClose {
		if c := m.Get(id); c != nil {
			_ = c.Close()
		}
	}
	return len(toClose)
}

func (m *Manager) PushUploaded(size int64) {
	m.uploadTemp.Add(size)
	m.uploadTotal.Add(size)
}

func (m *Manager) PushDownloaded(size int64) {
	m.downloadTemp.Add(size)
	m.downloadTotal.Add(size)
}

func (m *Manager) Now() (up int64, down int64) {
	return m.uploadBlip.Load(), m.downloadBlip.Load()
}

// Total is upload/download summed over connections whose leaf is not DIRECT/COMPATIBLE (chain.go).
// Per-connection stats in TrackerInfo are unchanged.
func (m *Manager) Total() (up, down int64) {
	return m.uploadTotal.Load(), m.downloadTotal.Load()
}

func (m *Manager) Memory() uint64 {
	m.updateMemory()
	return m.memory
}

func (m *Manager) Snapshot() *Snapshot {
	var connections []*TrackerInfo
	m.Range(func(c Tracker) bool {
		connections = append(connections, c.Info())
		return true
	})
	var recentClosed []*RecentClosedSnapshot
	for _, item := range cloneRecentClosed() {
		infoCopy := item.Info
		recentClosed = append(recentClosed, &RecentClosedSnapshot{
			TrackerInfo: &infoCopy,
			ClosedAt:    item.ClosedAt.UnixMilli(),
		})
	}
	return &Snapshot{
		UploadTotal:   m.uploadTotal.Load(),
		DownloadTotal: m.downloadTotal.Load(),
		Connections:   connections,
		Memory:        m.memory,
		RecentClosed:  recentClosed,
	}
}

func (m *Manager) updateMemory() {
	stat, err := memory.GetMemoryInfo(m.pid)
	if err != nil {
		return
	}
	m.memory = stat.RSS
}

func (m *Manager) ResetStatistic() {
	m.uploadTemp.Store(0)
	m.uploadBlip.Store(0)
	m.uploadTotal.Store(0)
	m.downloadTemp.Store(0)
	m.downloadBlip.Store(0)
	m.downloadTotal.Store(0)
}

func (m *Manager) handle() {
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		m.uploadBlip.Store(m.uploadTemp.Swap(0))
		m.downloadBlip.Store(m.downloadTemp.Swap(0))
	}
}

type Snapshot struct {
	DownloadTotal int64                    `json:"downloadTotal"`
	UploadTotal   int64                    `json:"uploadTotal"`
	Connections   []*TrackerInfo           `json:"connections"`
	RecentClosed  []*RecentClosedSnapshot  `json:"recentClosed,omitempty"`
	Memory        uint64                   `json:"memory"`
}

// RecentClosedSnapshot is a closed REJECT-family connection for clients that poll slower than connection lifetime.
type RecentClosedSnapshot struct {
	*TrackerInfo
	ClosedAt int64 `json:"closedAt"`
}
