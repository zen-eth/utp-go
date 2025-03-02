package utp_go

import (
	"sync"
	"testing"
)

// test for data race
func TestRandomUint16DataRace(t *testing.T) {
	var wg sync.WaitGroup
	const numGoroutines = 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_ = RandomUint16()
		}()
	}
	wg.Wait()
}