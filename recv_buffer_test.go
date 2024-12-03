package utp_go

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

const RECV_SIZE = 1024

func TestRecvBufferAvailable(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	if buf.Available() != RECV_SIZE {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE, buf.Available())
	}

	const DATA_LEN = 256
	data := make([]byte, DATA_LEN)
	for i := range data {
		data[i] = 0xef
	}

	// Write out-of-order packet.
	seqNum := initSeqNum + 2
	buf.Write(data, seqNum)
	if buf.Available() != RECV_SIZE-DATA_LEN {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE-DATA_LEN, buf.Available())
	}

	// Write in-order packet.
	seqNum = initSeqNum + 1
	buf.Write(data, seqNum)
	if buf.Available() != RECV_SIZE-(DATA_LEN*2) {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE-(DATA_LEN*2), buf.Available())
	}

	// Read all data.
	readBuf := make([]byte, DATA_LEN*2)
	n, err := buf.Read(readBuf)
	if err != nil || n != DATA_LEN*2 {
		t.Errorf("expected to read %d bytes, got %d, error: %v", DATA_LEN*2, n, err)
	}
	if buf.Available() != RECV_SIZE {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE, buf.Available())
	}
}

func TestWasWritten(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	seqNum := initSeqNum + 2
	if buf.WasWritten(seqNum) {
		t.Errorf("expected seqNum %d to not be written", seqNum)
	}

	const DATA_LEN = 64
	data := make([]byte, DATA_LEN)
	for i := range data {
		data[i] = 0xef
	}
	buf.Write(data, seqNum)
	if !buf.WasWritten(seqNum) {
		t.Errorf("expected seqNum %d to be written", seqNum)
	}
}

func TestRecvBufferWrite(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	const DATA_LEN = 256

	// Write out-of-order packet.
	dataSecond := make([]byte, DATA_LEN)
	for i := range dataSecond {
		dataSecond[i] = 0xef
	}
	seqNum := initSeqNum + 2
	buf.Write(dataSecond, seqNum)
	require.Equal(t, 0, buf.offset)
	require.Equal(t, uint16(0), buf.consumed)
	data, exist := buf.pending[seqNum]
	require.True(t, exist)
	require.True(t, bytes.Equal(dataSecond, data))

	// Write in-order packet.
	dataFirst := make([]byte, DATA_LEN)
	for i := range dataFirst {
		dataFirst[i] = 0xfe
	}
	seqNum = initSeqNum + 1
	buf.Write(dataFirst, seqNum)

	require.Equal(t, DATA_LEN*2, buf.offset)
	require.Equal(t, uint16(2), buf.consumed)
	require.Equal(t, 0, len(buf.pending))
	require.True(t, bytes.Equal(dataFirst, buf.buf[:DATA_LEN]))
	require.True(t, bytes.Equal(dataSecond, buf.buf[DATA_LEN:DATA_LEN*2]))
}

func TestRecvBufferWriteExceedsAvailable(t *testing.T) {

	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	seqNum := initSeqNum + 1
	data := make([]byte, RECV_SIZE+1)
	for i := range data {
		data[i] = 0xef
	}
	err := buf.Write(data, seqNum)
	require.Error(t, err)
}

func TestRecvBufferRead(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	const DATA_LEN = 256

	// Write out-of-order packet.
	dataSecond := make([]byte, DATA_LEN)
	for i := range dataSecond {
		dataSecond[i] = 0xef
	}
	seqNum := initSeqNum + 2
	buf.Write(dataSecond, seqNum)

	readBuf := make([]byte, RECV_SIZE)
	n, err := buf.Read(readBuf)
	if err != nil || n != 0 {
		t.Errorf("expected to read 0 bytes, got %d, error: %v", n, err)
	}

	// Write in-order packet.
	dataFirst := make([]byte, DATA_LEN)
	for i := range dataFirst {
		dataFirst[i] = 0xfe
	}
	seqNum = initSeqNum + 1
	buf.Write(dataFirst, seqNum)

	n, err = buf.Read(readBuf)
	if err != nil || n != DATA_LEN*2 {
		t.Errorf("expected to read %d bytes, got %d, error: %v", DATA_LEN*2, n, err)
	}
	if buf.offset != 0 {
		t.Errorf("expected buffer offset to be 0, got %d", buf.offset)
	}
	if !bytes.Equal(readBuf[:DATA_LEN], dataFirst) || !bytes.Equal(readBuf[DATA_LEN:DATA_LEN*2], dataSecond) {
		t.Errorf("unexpected data read from buffer")
	}

	n, err = buf.Read(readBuf)
	if err != nil || n != 0 {
		t.Errorf("expected to read 0 bytes, got %d, error: %v", n, err)
	}
}

func TestAckNum(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	if buf.AckNum() != initSeqNum {
		t.Errorf("expected ackNum to be %d, got %d", initSeqNum, buf.AckNum())
	}

	const DATA_LEN = 64
	data := make([]byte, DATA_LEN)
	for i := range data {
		data[i] = 0xef
	}

	// Write out-of-order packet.
	secondSeqNum := initSeqNum + 2
	buf.Write(data, secondSeqNum)

	if buf.AckNum() != initSeqNum {
		t.Errorf("expected ackNum to be %d, got %d", initSeqNum, buf.AckNum())
	}

	// Write in-order packet.
	firstSeqNum := initSeqNum + 1
	buf.Write(data, firstSeqNum)

	if buf.AckNum() != secondSeqNum {
		t.Errorf("expected ackNum to be %d, got %d", secondSeqNum, buf.AckNum())
	}
}

func TestSelectiveAck(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := NewReceiveBuffer(RECV_SIZE, initSeqNum)

	selectiveAck := buf.SelectiveAck()
	if selectiveAck != nil {
		t.Errorf("expected no selective ack, got %v", selectiveAck)
	}

	const DATA_LEN = 64
	data := make([]byte, DATA_LEN)
	for i := range data {
		data[i] = 0xef
	}

	// Write out-of-order packet.
	seqNum := initSeqNum + 2
	buf.Write(data, seqNum)

	selectiveAck = buf.SelectiveAck()
	if selectiveAck == nil || !selectiveAck.Acked()[0] {
		t.Errorf("expected selective ack with first packet acked, got %v", selectiveAck)
	}

	// Write in-order packet.
	seqNum = initSeqNum + 1
	buf.Write(data, seqNum)

	selectiveAck = buf.SelectiveAck()
	if selectiveAck != nil {
		t.Errorf("expected no selective ack, got %v", selectiveAck)
	}
}
