package utp_go

import "errors"

type ReceiveBuffer struct {
	buf        []byte
	offset     int
	pending    map[uint16][]byte
	initSeqNum uint16
	consumed   uint16
}

func NewReceiveBuffer(size int, initSeqNum uint16) *ReceiveBuffer {
	return &ReceiveBuffer{
		buf:        make([]byte, size),
		offset:     0,
		pending:    make(map[uint16][]byte),
		initSeqNum: initSeqNum,
		consumed:   0,
	}
}

func (rb *ReceiveBuffer) Available() int {
	available := len(rb.buf) - rb.offset
	for _, data := range rb.pending {
		available -= len(data)
	}
	return available
}

func (rb *ReceiveBuffer) IsEmpty() bool {
	return rb.offset == 0 && len(rb.pending) == 0
}

func (rb *ReceiveBuffer) InitSeqNum() uint16 {
	return rb.initSeqNum
}

func (rb *ReceiveBuffer) WasWritten(seqNum uint16) bool {
	_, exists := rb.pending[seqNum]
	return exists || (seqNum >= rb.initSeqNum && seqNum < rb.initSeqNum+rb.consumed)
}

func (rb *ReceiveBuffer) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	n := minInt(len(buf), rb.offset)
	copy(buf, rb.buf[:n])

	remaining := rb.offset - n
	copy(rb.buf, rb.buf[n:n+remaining])
	rb.offset = remaining

	return n, nil
}

func (rb *ReceiveBuffer) Write(data []byte, seqNum uint16) error {
	if rb.WasWritten(seqNum) {
		return nil
	}

	if len(data) > rb.Available() {
		return errors.New("insufficient space in buffer")
	}

	rb.pending[seqNum] = append([]byte(nil), data...)

	start := rb.initSeqNum + 1
	next := start + rb.consumed
	for {
		data, exists := rb.pending[next]
		if !exists {
			break
		}

		end := rb.offset + len(data)
		copy(rb.buf[rb.offset:end], data)
		rb.offset = end
		rb.consumed++
		delete(rb.pending, next)
		next++
	}

	return nil
}

func (rb *ReceiveBuffer) AckNum() uint16 {
	return rb.initSeqNum + rb.consumed
}

func (rb *ReceiveBuffer) SelectiveAck() []bool {
	if len(rb.pending) == 0 {
		return nil
	}

	next := rb.AckNum() + 2
	acked := make([]bool, 0)

	for seqNum := range rb.pending {
		for seqNum != next {
			acked = append(acked, false)
			next++
		}
		acked = append(acked, true)
		next++
	}

	return acked
}
