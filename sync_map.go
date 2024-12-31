package utp_go

import "sync"

type syncMap[P any] struct {
	mu    sync.Mutex
	inner map[any]P
}

func newSyncMap[P any]() *syncMap[P] {
	return &syncMap[P]{
		inner: make(map[any]P),
	}
}

func (m *syncMap[P]) get(key any) (P, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.inner[key]
	return p, ok
}

func (m *syncMap[P]) put(key any, value P) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inner[key] = value
}

func (m *syncMap[P]) remove(key any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inner, key)
}

func (m *syncMap[P]) Range(f func(key any, value P) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, v := range m.inner {
		if !f(k, v) {
			break
		}
	}
}
