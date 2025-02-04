package utp_go

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

const RECV_SIZE = 1024

func TestRecvBufferAvailable(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

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
	err := buf.Write(data, seqNum)
	require.NoError(t, err, "failed to write data")
	if buf.Available() != RECV_SIZE-DATA_LEN {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE-DATA_LEN, buf.Available())
	}

	// Write in-order packet.
	seqNum = initSeqNum + 1
	err = buf.Write(data, seqNum)
	require.NoError(t, err, "failed to write data")
	if buf.Available() != RECV_SIZE-(DATA_LEN*2) {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE-(DATA_LEN*2), buf.Available())
	}

	// Read all data.
	readBuf := make([]byte, DATA_LEN*2)
	n := buf.Read(readBuf)
	if n != DATA_LEN*2 {
		t.Errorf("expected to read %d bytes, got %d", DATA_LEN*2, n)
	}
	if buf.Available() != RECV_SIZE {
		t.Errorf("expected available size to be %d, got %d", RECV_SIZE, buf.Available())
	}
}

func TestWasWritten(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

	seqNum := initSeqNum + 2
	if buf.WasWritten(seqNum) {
		t.Errorf("expected seqNum %d to not be written", seqNum)
	}

	const DATA_LEN = 64
	data := make([]byte, DATA_LEN)
	for i := range data {
		data[i] = 0xef
	}
	err := buf.Write(data, seqNum)
	require.NoError(t, err, "failed to write data")
	if !buf.WasWritten(seqNum) {
		t.Errorf("expected seqNum %d to be written", seqNum)
	}
}

func TestRecvBufferWrite(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

	const DATA_LEN = 256

	// Write out-of-order packet.
	dataSecond := make([]byte, DATA_LEN)
	for i := range dataSecond {
		dataSecond[i] = 0xef
	}
	seqNum := initSeqNum + 2
	err := buf.Write(dataSecond, seqNum)
	require.NoError(t, err, "failed to write data")
	require.Equal(t, 0, buf.offset)
	require.Equal(t, uint16(0), buf.consumed)
	data := buf.pending.Get(&pendingItem{seqNum: seqNum}).(*pendingItem)
	require.NotNil(t, data)
	require.True(t, bytes.Equal(dataSecond, data.data))

	// Write in-order packet.
	dataFirst := make([]byte, DATA_LEN)
	for i := range dataFirst {
		dataFirst[i] = 0xfe
	}
	seqNum = initSeqNum + 1
	err = buf.Write(dataFirst, seqNum)
	require.NoError(t, err, "failed to write data")

	require.Equal(t, DATA_LEN*2, buf.offset)
	require.Equal(t, uint16(2), buf.consumed)
	require.Equal(t, 0, buf.pending.Len())
	require.True(t, bytes.Equal(dataFirst, buf.buf[:DATA_LEN]))
	require.True(t, bytes.Equal(dataSecond, buf.buf[DATA_LEN:DATA_LEN*2]))
}

func TestRecvBufferWriteExceedsAvailable(t *testing.T) {

	initSeqNum := uint16(0xFFFF)
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

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
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

	const DATA_LEN = 256

	// Write out-of-order packet.
	dataSecond := make([]byte, DATA_LEN)
	for i := range dataSecond {
		dataSecond[i] = 0xef
	}
	seqNum := initSeqNum + 2
	err := buf.Write(dataSecond, seqNum)
	require.NoError(t, err, "failed to write data")

	readBuf := make([]byte, RECV_SIZE)
	n := buf.Read(readBuf)
	if n != 0 {
		t.Errorf("expected to read 0 bytes, got %d", n)
	}

	// Write in-order packet.
	dataFirst := make([]byte, DATA_LEN)
	for i := range dataFirst {
		dataFirst[i] = 0xfe
	}
	seqNum = initSeqNum + 1
	err = buf.Write(dataFirst, seqNum)
	require.NoError(t, err, "failed to write data")

	n = buf.Read(readBuf)
	if n != DATA_LEN*2 {
		t.Errorf("expected to read %d bytes, got %d", DATA_LEN*2, n)
	}
	if buf.offset != 0 {
		t.Errorf("expected buffer offset to be 0, got %d", buf.offset)
	}
	if !bytes.Equal(readBuf[:DATA_LEN], dataFirst) || !bytes.Equal(readBuf[DATA_LEN:DATA_LEN*2], dataSecond) {
		t.Errorf("unexpected data read from buffer")
	}

	n = buf.Read(readBuf)
	if n != 0 {
		t.Errorf("expected to read 0 bytes, got %d", n)
	}
}

func TestAckNum(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

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
	err := buf.Write(data, secondSeqNum)
	require.NoError(t, err, "failed to write data to receive buffer")

	if buf.AckNum() != initSeqNum {
		t.Errorf("expected ackNum to be %d, got %d", initSeqNum, buf.AckNum())
	}

	// Write in-order packet.
	firstSeqNum := initSeqNum + 1
	err = buf.Write(data, firstSeqNum)
	require.NoError(t, err, "failed to write data to receive buffer")

	if buf.AckNum() != secondSeqNum {
		t.Errorf("expected ackNum to be %d, got %d", secondSeqNum, buf.AckNum())
	}
}

func TestSelectiveAck(t *testing.T) {
	initSeqNum := uint16(0xFFFF)
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

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
	err := buf.Write(data, seqNum)
	require.NoError(t, err, "failed to write data to receive buffer")

	selectiveAck = buf.SelectiveAck()
	if selectiveAck == nil || !selectiveAck.Acked()[0] {
		t.Errorf("expected selective ack with first packet acked, got %v", selectiveAck)
	}

	// Write in-order packet.
	seqNum = initSeqNum + 1
	err = buf.Write(data, seqNum)
	require.NoError(t, err, "failed to write data to receive buffer")

	selectiveAck = buf.SelectiveAck()
	if selectiveAck != nil {
		t.Errorf("expected no selective ack, got %v", selectiveAck)
	}
}

func TestSelectiveACKOverflow(t *testing.T) {
	initSeqNum := uint16(math.MaxUint16) - 2
	buf := newReceiveBuffer(RECV_SIZE, initSeqNum)

	// 初始时无选择性ACK
	nilSelectiveAck := buf.SelectiveAck()
	require.Nil(t, nilSelectiveAck, "Expected no selective ACK initially")

	data := make([]byte, 64)
	for i := range data {
		data[i] = 0xef
	}

	// 写入乱序包（注意Go中uint16自动处理溢出）
	seqNum := uint16(initSeqNum + 2) // 65533 + 2 = 65535 (overflow)
	buf.Write(data, seqNum)

	seqNum = initSeqNum + 3 // 65533 + 3 = 0 (overflow)
	buf.Write(data, seqNum)

	seqNum = initSeqNum + 5 // 65534 + 5 = 2 (overflow)
	buf.Write(data, seqNum)

	// 验证选择性ACK
	notNilSelectiveAck := buf.SelectiveAck()
	require.NotNil(t, notNilSelectiveAck, "Expected a selective ACK initially")
	acked := notNilSelectiveAck.Acked()
	require.NotNil(t, acked, "Expected a acked list")

	// 构造期望的确认位图
	expected := make([]bool, 32)
	expected[0] = true // 对应序列号0（起始0+0）
	expected[1] = true // 对应序列号1（起始0+1）
	expected[3] = true // 对应序列号3（起始0+3）

	require.True(t, reflect.DeepEqual(notNilSelectiveAck.Acked(), expected),
		"Expected acked %v, got %v", expected, notNilSelectiveAck.Acked())
}
