package utp_go

import (
	"errors"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/btree"
)

const (
	MAX_SELECTIVE_ACK_COUNT int = 32 * 63
)

var (
	bufferPools = make(map[int]*sync.Pool)
)

type receiveBuffer struct {
	logger     log.Logger
	buf        []byte
	offset     int
	pending    *btree.BTree
	initSeqNum uint16
	consumed   uint16
}

type pendingItem struct {
	seqNum uint16
	data   []byte
}

func (i *pendingItem) Less(other btree.Item) bool {
	return i.seqNum < other.(*pendingItem).seqNum
}

func createOrGetPool(size int) *sync.Pool {
	if pool, exist := bufferPools[size]; exist {
		return pool
	}
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, size)
		},
	}
	bufferPools[size] = pool
	return pool
}

func newReceiveBuffer(size int, initSeqNum uint16) *receiveBuffer {
	//buf := createOrGetPool(size).Get().([]byte)
	buf := make([]byte, size)
	return &receiveBuffer{
		buf:        buf,
		offset:     0,
		pending:    btree.New(2),
		initSeqNum: initSeqNum,
		consumed:   0,
	}
}

func newReceiveBufferWithLogger(size int, initSeqNum uint16, logger log.Logger) *receiveBuffer {
	//buf := createOrGetPool(size).Get().([]byte)
	buf := make([]byte, size)
	return &receiveBuffer{
		logger:     logger,
		buf:        buf,
		offset:     0,
		pending:    btree.New(2),
		initSeqNum: initSeqNum,
		consumed:   0,
	}
}

func (rb *receiveBuffer) Available() int {
	available := len(rb.buf) - rb.offset

	rb.pending.Ascend(func(i btree.Item) bool {
		item := i.(*pendingItem)
		available -= len(item.data)
		return true
	})
	return available
}

func (rb *receiveBuffer) IsEmpty() bool {
	return rb.offset == 0 && rb.pending.Len() == 0
}

func (rb *receiveBuffer) InitSeqNum() uint16 {
	return rb.initSeqNum
}

func (rb *receiveBuffer) WasWritten(seqNum uint16) bool {
	exists := rb.pending.Has(&pendingItem{seqNum: seqNum})
	if rb.logger != nil && rb.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		rb.logger.Trace("checking written", "seqNum", seqNum, "initSeqNum", rb.initSeqNum, "consumed", rb.consumed, "exists", exists)
	}
	writtenRange := circularRangeInclusive{start: rb.initSeqNum, end: rb.initSeqNum + rb.consumed}
	return exists || writtenRange.Contains(seqNum)
}

func (rb *receiveBuffer) Read(buf []byte) int {
	if len(buf) == 0 {
		return 0
	}

	n := minInt(len(buf), rb.offset)
	copy(buf, rb.buf[:n])

	remaining := rb.offset - n
	copy(rb.buf, rb.buf[n:n+remaining])
	rb.offset = remaining

	return n
}

func (rb *receiveBuffer) Write(data []byte, seqNum uint16) error {
	if rb.WasWritten(seqNum) {
		return nil
	}
	if rb.logger != nil && rb.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		rb.logger.Trace("will put a data to recv buffer", "seq", seqNum)
	}
	if len(data) > rb.Available() {
		return errors.New("insufficient space in buffer")
	}

	rb.pending.ReplaceOrInsert(&pendingItem{seqNum: seqNum, data: data})

	//start := rb.initSeqNum + 1
	next := rb.initSeqNum + 1 + rb.consumed
	if rb.logger != nil && rb.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		rb.logger.Trace("will handle pending data in recv buffer", "startSeq", next)
	}

	for {
		item := rb.pending.Get(&pendingItem{seqNum: next})
		if item == nil {
			break
		}

		pending := item.(*pendingItem)

		end := rb.offset + len(pending.data)
		copy(rb.buf[rb.offset:end], pending.data)
		rb.offset = end
		rb.consumed += 1
		rb.pending.Delete(pending)
		if rb.logger != nil && rb.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			rb.logger.Trace("will delete a pending data in recv buffer", "seq", next, "pending.len", rb.pending.Len())
		}
		next += 1
	}
	if rb.logger != nil && rb.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		rb.logger.Trace("handled pending data in recv buffer", "endSeq", next)
	}
	return nil
}

func (rb *receiveBuffer) AckNum() uint16 {
	return rb.initSeqNum + rb.consumed
}

func (rb *receiveBuffer) SelectiveAck() *SelectiveAck {
	if rb.pending.Len() == 0 {
		return nil
	}

	lastAck := rb.AckNum() + 2
	acked := make([]bool, 0)

	pendingSeqs := make(map[uint16]bool, rb.pending.Len())
	rb.pending.Ascend(func(i btree.Item) bool {
		item := i.(*pendingItem)
		pendingSeqs[item.seqNum] = true
		return true
	})

	for len(pendingSeqs) != 0 && len(acked) < MAX_SELECTIVE_ACK_COUNT {
		if _, ok := pendingSeqs[lastAck]; ok {
			acked = append(acked, true)
			delete(pendingSeqs, lastAck)
		} else {
			acked = append(acked, false)
		}
		lastAck++
	}

	if rb.logger != nil && rb.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		rb.logger.Trace("will new selective ack", "endSeq", lastAck, "acked.len", len(acked))
	}

	return NewSelectiveAck(acked)
}

func (rb *receiveBuffer) close() {
	createOrGetPool(len(rb.buf)).Put(rb.buf)
}
