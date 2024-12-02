package utp_go

import (
	"github.com/protolambda/zrnt/eth2/util/math"
)

type GrowableCircularBuffer struct {
	Items []any
	Mask  uint32
}

func NewBuffer(size uint32) *GrowableCircularBuffer {
	powOfTwoSize := uint32(math.NextPowerOfTwo(uint64(size)))
	return &GrowableCircularBuffer{
		Items: make([]any, size),
		Mask:  powOfTwoSize - 1,
	}
}

func (b *GrowableCircularBuffer) Get(i uint32) any {
	return b.Items[i&b.Mask]
}

func (b *GrowableCircularBuffer) Put(i uint32, elem any) {
	b.Items[i&b.Mask] = elem
}

func (b *GrowableCircularBuffer) Delete(i uint32) {
	b.Put(i, nil)
}

func (b *GrowableCircularBuffer) HasKey(i uint32) bool {
	return b.Get(i) != nil
}

func (b *GrowableCircularBuffer) Len() int {
	return int(b.Mask) + 1
}

func calculateNextMask(currentMask uint32, index uint32) uint32 {
	maxUint32 := ^uint32(0)
	if currentMask == maxUint32 {
		return currentMask
	}
	var newSize = currentMask + 1
	for {
		newSize = newSize * 2
		if newSize == 0 || index < newSize {
			break
		}
	}
	return newSize - 1
}

func (b *GrowableCircularBuffer) ensureSize(item uint32, index uint32) {
	if index <= b.Mask {
		return
	}
	newMask := calculateNextMask(b.Mask, index)
	newItems := make([]any, newMask+1)
	i := uint32(0)
	for ; i <= b.Mask; i++ {
		idx := item - index + i
		newItems[idx&newMask] = b.Get(idx)
	}
	b.Items = newItems
	b.Mask = newMask
}
