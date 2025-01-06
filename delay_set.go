package utp_go

import (
	"sync"
	"time"
)

type expireFunc[P any] func(key any, value P)

type timeWheelItem[P any] struct {
	value P
	key   any
}

type timeWheel[P any] struct {
	stopped          chan struct{}
	interval         time.Duration
	slots            []map[any]*timeWheelItem[P]
	ticker           *time.Ticker
	current          int
	slotNum          int
	maxDelay         time.Duration
	handleExpireFunc expireFunc[P]
	mu               sync.RWMutex
}

func newTimeWheel[P any](interval time.Duration, slotNum int, handleExpireFunc expireFunc[P]) *timeWheel[P] {
	tw := &timeWheel[P]{
		stopped:          make(chan struct{}),
		interval:         interval,
		slots:            make([]map[any]*timeWheelItem[P], slotNum),
		current:          0,
		slotNum:          slotNum,
		maxDelay:         time.Duration(slotNum) * interval,
		handleExpireFunc: handleExpireFunc,
	}

	for i := 0; i < slotNum; i++ {
		tw.slots[i] = make(map[any]*timeWheelItem[P])
	}

	tw.ticker = time.NewTicker(interval)
	go tw.run()
	return tw
}

func (tw *timeWheel[P]) put(key any, value P, delay time.Duration) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if delay > tw.maxDelay {
		delay = tw.maxDelay
	}

	slots := int(delay / tw.interval)
	index := (tw.current + slots) % tw.slotNum
	tw.slots[index][key] = &timeWheelItem[P]{
		key:   key,
		value: value,
	}
}

func (tw *timeWheel[P]) retain(shouldRemove func(key any) bool) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	for i := 0; i < tw.slotNum; i++ {
		for key := range tw.slots[i] {
			if shouldRemove(key) {
				delete(tw.slots[i], key)
			}
		}
	}
}

func (tw *timeWheel[P]) remove(key any) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	for i := 0; i < tw.slotNum; i++ {
		if _, exists := tw.slots[i][key]; exists {
			delete(tw.slots[i], key)
			return
		}
	}
}

func (tw *timeWheel[P]) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.mu.Lock()
			currentSlot := tw.slots[tw.current]
			// clear current slot
			if len(currentSlot) > 0 {
				for key, item := range currentSlot {
					// process expirations here
					delete(currentSlot, key)
					tw.handleExpireFunc(key, item.value)
				}
			}
			tw.current = (tw.current + 1) % tw.slotNum
			tw.mu.Unlock()
		case <-tw.stopped:
			return
		}
	}
}

func (tw *timeWheel[P]) Len() int {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	length := 0
	for i := 0; i < tw.slotNum; i++ {
		length += len(tw.slots[i])
	}
	return length
}

func (tw *timeWheel[P]) stop() {
	tw.ticker.Stop()
	close(tw.stopped)
}
