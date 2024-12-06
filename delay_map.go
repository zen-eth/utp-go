package utp_go

import (
	"sync"
	"time"
)

type DelayMap[P any] struct {
	unackedMu     sync.Mutex
	unacked       map[any]*delayItem[P]
	itemTimeoutCh chan *delayItem[P]
}

type delayItem[P any] struct {
	Item  P
	Key   any
	timer *time.Timer
}

func NewDelayMap[P any]() *DelayMap[P] {
	return &DelayMap[P]{
		unacked:       make(map[any]*delayItem[P]),
		itemTimeoutCh: make(chan *delayItem[P], 1),
	}
}

func (m *DelayMap[P]) timeoutCh() chan *delayItem[P] {
	return m.itemTimeoutCh
}

func (m *DelayMap[P]) Put(key any, value P, timeout time.Duration) {
	m.unackedMu.Lock()
	defer m.unackedMu.Unlock()

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

func (m *DelayMap[P]) Get(key any) P {
	m.unackedMu.Lock()
	defer m.unackedMu.Unlock()
	return m.unacked[key].Item
}

func (m *DelayMap[P]) Remove(key any) {
	m.unackedMu.Lock()
	defer m.unackedMu.Unlock()
	if item, ok := m.unacked[key]; ok {
		item.timer.Stop()
	}
	delete(m.unacked, key)
}
