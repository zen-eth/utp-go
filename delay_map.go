package utp_go

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

type delayMap[P any] struct {
	lock          sync.Mutex
	innerMap      map[any]*delayItem[P]
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
		innerMap:      make(map[any]*delayItem[P]),
		itemTimeoutCh: make(chan *delayItem[P], 100),
		shouldLock:    false,
	}
}

func (m *delayMap[P]) timeoutCh() chan *delayItem[P] {
	return m.itemTimeoutCh
}

func (m *delayMap[P]) put(key any, value P, timeout time.Duration) {
	if m.shouldLock {
		m.lock.Lock()
		defer m.lock.Unlock()
	}

	item := &delayItem[P]{
		Key:  key,
		Item: value,
	}
	timer := time.AfterFunc(timeout, func() {
		m.itemTimeoutCh <- item
	})
	item.timer = timer
	m.innerMap[key] = item
}

func (m *delayMap[P]) get(key any) P {
	if m.shouldLock {
		m.lock.Lock()
		defer m.lock.Unlock()
	}
	return m.innerMap[key].Item
}

func (m *delayMap[P]) retain(shouldRemove func(key any) bool) {
	if m.shouldLock {
		m.lock.Lock()
		defer m.lock.Unlock()
	}
	for k := range m.innerMap {
		if shouldRemove(k) {
			m.remove(k)
		}
	}
}

func (m *delayMap[P]) remove(key any) {
	if m.shouldLock {
		m.lock.Lock()
		defer m.lock.Unlock()
	}
	log.Debug("delay map key count before", "count", len(m.innerMap))
	if item, ok := m.innerMap[key]; ok {
		item.timer.Stop()
	}
	delete(m.innerMap, key)
	log.Debug("delay map key count after", "count", len(m.innerMap))
}

func (m *delayMap[P]) keys() []any {
	if m.shouldLock {
		m.lock.Lock()
		defer m.lock.Unlock()
	}
	var keys []any
	for k := range m.innerMap {
		keys = append(keys, k)
	}
	return keys
}
