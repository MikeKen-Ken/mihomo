package outboundgroup

import (
	"sync"

	"github.com/metacubex/mihomo/component/profile/cachefile"
)

type manualSelectionSnapshot struct {
	name       string
	generation uint64
}

type manualSelectionState struct {
	mu         sync.RWMutex
	name       string
	generation uint64
}

func (s *manualSelectionState) snapshot() manualSelectionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return manualSelectionSnapshot{name: s.name, generation: s.generation}
}

func (s *manualSelectionState) set(name string) {
	s.mu.Lock()
	s.name = name
	s.generation++
	s.mu.Unlock()
}

func (s *manualSelectionState) clear() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.name == "" {
		return false
	}
	s.name = ""
	s.generation++
	return true
}

func (s *manualSelectionState) clearIfUnchanged(expected manualSelectionSnapshot) bool {
	if expected.name == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.name != expected.name || s.generation != expected.generation {
		return false
	}
	s.name = ""
	s.generation++
	return true
}

type manualSelectionPersistence interface {
	Clear(groupName string)
}

type cacheManualSelectionPersistence struct{}

func (cacheManualSelectionPersistence) Clear(groupName string) {
	cachefile.Cache().SetSelected(groupName, "")
}

func (gb *GroupBase) onManualSelectionCleared() {
	if gb.selectionPersistence != nil {
		gb.selectionPersistence.Clear(gb.Name())
	}
	notifyProxyGroupRefresh(gb.Name())
}
