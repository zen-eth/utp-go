package utp_go

import (
	"errors"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/btree"
)

type ReceiveBuffer struct {
	logger     log.Logger
	buf        []byte
	offset     int
	pending    *btree.BTree
	initSeqNum uint16
	consumed   uint16
}

type PendingItem struct {
	seqNum uint16
	data   []byte
}

func (i *PendingItem) Less(other btree.Item) bool {
	return i.seqNum < other.(*PendingItem).seqNum
}

func NewReceiveBuffer(size int, initSeqNum uint16) *ReceiveBuffer {
	return &ReceiveBuffer{
		buf:        make([]byte, size),
		offset:     0,
		pending:    btree.New(2),
		initSeqNum: initSeqNum,
		consumed:   0,
	}
}

func NewReceiveBufferWithLogger(size int, initSeqNum uint16, logger log.Logger) *ReceiveBuffer {
	return &ReceiveBuffer{
		logger:  logger,
		buf:     make([]byte, size),
		offset:  0,
		pending: btree.New(2),
		//pending:      make(map[uint16][]byte),
		initSeqNum: initSeqNum,
		consumed:   0,
	}
}

func (rb *ReceiveBuffer) Available() int {
	available := len(rb.buf) - rb.offset

	rb.pending.Ascend(func(i btree.Item) bool {
		item := i.(*PendingItem)
		available -= len(item.data)
		return true
	})
	return available
}

func (rb *ReceiveBuffer) IsEmpty() bool {
	return rb.offset == 0 && rb.pending.Len() == 0
}

func (rb *ReceiveBuffer) InitSeqNum() uint16 {
	return rb.initSeqNum
}

func (rb *ReceiveBuffer) WasWritten(seqNum uint16) bool {
	exists := rb.pending.Has(&PendingItem{seqNum: seqNum})
	rb.logger.Debug("checking written", "seqNum", seqNum, "initSeqNum", rb.initSeqNum, "consumed", rb.consumed, "exists", exists)
	writtenRange := CircularRangeInclusive{start: rb.initSeqNum, end: rb.initSeqNum + rb.consumed}
	return exists || writtenRange.Contains(seqNum)
}

func (rb *ReceiveBuffer) Read(buf []byte) int {
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

func (rb *ReceiveBuffer) Write(data []byte, seqNum uint16) error {
	if rb.WasWritten(seqNum) {
		return nil
	}
	if rb.logger != nil {
		rb.logger.Debug("will put a data to recv buffer", "seq", seqNum)
	}
	if len(data) > rb.Available() {
		return errors.New("insufficient space in buffer")
	}

	rb.pending.ReplaceOrInsert(&PendingItem{seqNum: seqNum, data: data})

	//start := rb.initSeqNum + 1
	next := rb.initSeqNum + 1 + rb.consumed
	if rb.logger != nil {
		rb.logger.Debug("will handle pending data in recv buffer", "startSeq", next)
	}

	for {
		item := rb.pending.Get(&PendingItem{seqNum: next})
		if item == nil {
			break
		}

		pending := item.(*PendingItem)

		end := rb.offset + len(pending.data)
		copy(rb.buf[rb.offset:end], pending.data)
		rb.offset = end
		rb.consumed += 1
		rb.pending.Delete(pending)
		if rb.logger != nil {
			rb.logger.Debug("will delete a pending data in recv buffer", "seq", next, "pending.len", rb.pending.Len())
		}
		next += 1
	}
	if rb.logger != nil {
		rb.logger.Debug("handled pending data in recv buffer", "endSeq", next)
	}
	return nil
}

func (rb *ReceiveBuffer) AckNum() uint16 {
	return rb.initSeqNum + rb.consumed
}

func (rb *ReceiveBuffer) SelectiveAck() *SelectiveAck {
	if rb.pending.Len() == 0 {
		return nil
	}

	next := rb.AckNum() + 2
	acked := make([]bool, 0)

	rb.pending.Ascend(func(i btree.Item) bool {
		item := i.(*PendingItem)
		for item.seqNum != next {
			acked = append(acked, false)
			next++
		}
		acked = append(acked, true)
		next++
		return true
	})

	if rb.logger != nil {
		rb.logger.Debug("will new selective ack", "endSeq", next, "acked.len", len(acked))
	}

	return NewSelectiveAck(acked)
}
