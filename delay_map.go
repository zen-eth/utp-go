package utp_go

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

type delayMap[P any] struct {
	unackedMu     sync.Mutex
	unacked       map[any]*delayItem[P]
	itemTimeoutCh chan *delayItem[P]
	shouldLock    bool
}

type delayItem[P any] struct {
	Item  P
	Key   any
	timer *time.Timer
}

func newDelayMap[P any]() *delayMap[P] {
	return &delayMap[P]{
		unacked:       make(map[any]*delayItem[P]),
		itemTimeoutCh: make(chan *delayItem[P], 100),
		shouldLock:    false,
	}
}

func (m *delayMap[P]) timeoutCh() chan *delayItem[P] {
	return m.itemTimeoutCh
}

func (m *delayMap[P]) Put(key any, value P, timeout time.Duration) {
	if m.shouldLock {
		m.unackedMu.Lock()
		defer m.unackedMu.Unlock()
	}

	item := &delayItem[P]{
		Key:  key,
		Item: value,
	}
	timer := time.AfterFunc(timeout, func() {
		m.itemTimeoutCh <- item
	})
	item.timer = timer
	m.unacked[key] = item
}

func (m *delayMap[P]) Get(key any) P {
	if m.shouldLock {
		m.unackedMu.Lock()
		defer m.unackedMu.Unlock()
	}
	return m.unacked[key].Item
}

func (m *delayMap[P]) Retain(shouldRemove func(key any) bool) {
	if m.shouldLock {
		m.unackedMu.Lock()
		defer m.unackedMu.Unlock()
	}
	for k := range m.unacked {
		if shouldRemove(k) {
			m.Remove(k)
		}
	}
}

func (m *delayMap[P]) Remove(key any) {
	if m.shouldLock {
		m.unackedMu.Lock()
		defer m.unackedMu.Unlock()
	}
	log.Debug("delay map key count before", "count", len(m.unacked))
	if item, ok := m.unacked[key]; ok {
		item.timer.Stop()
	}
	delete(m.unacked, key)
	log.Debug("delay map key count after", "count", len(m.unacked))
}

func (m *delayMap[P]) Keys() []any {
	if m.shouldLock {
		m.unackedMu.Lock()
		defer m.unackedMu.Unlock()
	}
	var keys []any
	for k := range m.unacked {
		keys = append(keys, k)
	}
	return keys
}
