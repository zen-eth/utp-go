package utp_go

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOnTransmit(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())
	initialTimeout := ctrl.Timeout()

	// Register the initial transmission of a packet with sequence number 1
	seqNum := uint16(1)
	packetOneSizeBytes := uint32(32)

	err := ctrl.OnTransmit(seqNum, Initial, packetOneSizeBytes)
	require.NoError(t, err, "transmission registration failed")

	transmissionRecord, exists := ctrl.transmissions[seqNum]
	require.True(t, exists, "transmission not recorded")
	require.Equal(t, packetOneSizeBytes, transmissionRecord.SizeBytes,
		"expected size %d, got %d", packetOneSizeBytes, transmissionRecord.SizeBytes)
	require.Equal(t, uint32(1), transmissionRecord.NumTransmissions,
		"expected 1 transmission, got %d", transmissionRecord.NumTransmissions)

	require.Equal(t, packetOneSizeBytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", packetOneSizeBytes, ctrl.windowSizeBytes)

	// Register the initial transmission of a packet with sequence number 2
	seqNum = 2
	packetTwoSizeBytes := uint32(128)

	err = ctrl.OnTransmit(seqNum, Initial, packetTwoSizeBytes)
	require.NoError(t, err, "transmission registration failed")

	transmissionRecord, exists = ctrl.transmissions[seqNum]
	require.True(t, exists, "transmission not recorded")
	require.Equal(t, packetTwoSizeBytes, transmissionRecord.SizeBytes,
		"expected size %d, got %d", packetOneSizeBytes, transmissionRecord.SizeBytes)
	require.Equal(t, uint32(1), transmissionRecord.NumTransmissions,
		"expected 1 transmission, got %d", transmissionRecord.NumTransmissions)

	require.Equal(t, packetOneSizeBytes+packetTwoSizeBytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", packetOneSizeBytes+packetTwoSizeBytes, ctrl.windowSizeBytes)

	// Register the retransmission of the packet with sequence number 2
	err = ctrl.OnTransmit(seqNum, Retransmission, 0)
	require.NoError(t, err, "transmission registration failed")

	transmissionRecord, exists = ctrl.transmissions[seqNum]
	require.True(t, exists, "transmission not recorded")
	require.Equal(t, packetTwoSizeBytes, transmissionRecord.SizeBytes,
		"expected size %d, got %d", packetOneSizeBytes, transmissionRecord.SizeBytes)
	require.Equal(t, uint32(2), transmissionRecord.NumTransmissions,
		"expected 1 transmission, got %d", transmissionRecord.NumTransmissions)

	require.Equal(t, packetOneSizeBytes+packetTwoSizeBytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", packetOneSizeBytes+packetTwoSizeBytes, ctrl.windowSizeBytes)

	if ctrl.Timeout() != initialTimeout {
		t.Fatalf("expected timeout %v, got %v", initialTimeout, ctrl.Timeout())
	}
}

func TestOnTransmitDuplicateTransmission(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Register the initial transmission of a packet with sequence number 1
	seqNum := uint16(1)
	bytes := uint32(32)

	err := ctrl.OnTransmit(seqNum, Initial, bytes)
	require.NoError(t, err, "transmission registration failed")
	require.Equal(t, bytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", bytes, ctrl.windowSizeBytes)

	// Register the initial transmission of the SAME packet
	err = ctrl.OnTransmit(seqNum, Initial, bytes)
	require.ErrorIs(t, err, ErrDuplicateTransmission, "expected ErrDuplicateTransmission, got %v", err)
	require.Equal(t, bytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", bytes, ctrl.windowSizeBytes)
}

func TestOnTransmitUnknownSeqNum(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Register the retransmission of the packet with sequence number 1
	seqNum := uint16(1)
	err := ctrl.OnTransmit(seqNum, Retransmission, 0)
	require.ErrorIs(t, err, ErrUnknownSeqNum, "expected ErrUnknownSeqNum, got %v", err)
	require.Equal(t, uint32(0), ctrl.windowSizeBytes, "expected window size 0, got %d", ctrl.windowSizeBytes)
}

func TestOnTransmitInsufficientWindowSize(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Register the transmission of a packet with sequence number 1
	seqNum := uint16(1)
	bytes := ctrl.maxWindowSizeBytes + 1

	err := ctrl.OnTransmit(seqNum, Initial, bytes)
	require.ErrorIs(t, err, ErrInsufficientWindowSize,
		"expected ErrInsufficientWindowSize, got %v", err)

	require.Equal(t, uint32(0), ctrl.windowSizeBytes,
		"expected window size 0, got %d", ctrl.windowSizeBytes)
}

func TestOnAck(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Register the initial transmission
	seqNum := uint16(1)
	bytes := uint32(32)

	err := ctrl.OnTransmit(seqNum, Initial, bytes)
	require.NoError(t, err, "transmission registration failed")

	// Register the acknowledgement
	ackDelay := 150 * time.Millisecond
	ackRTT := 300 * time.Millisecond
	ackReceivedAt := time.Now()
	ack := Ack{
		Delay:      ackDelay,
		RTT:        ackRTT,
		ReceivedAt: ackReceivedAt,
	}

	err = ctrl.OnAck(seqNum, ack)
	require.NoError(t, err, "ack registration failed")

	// Check base delay
	baseDelay := ctrl.delayAcc.BaseDelay()
	require.Equal(t, ackDelay, baseDelay, "expected base delay %v, got %v", ackDelay, baseDelay)
	// Check window size
	require.Equal(t, uint32(0), ctrl.windowSizeBytes, "expected window size 0, got %d", ctrl.windowSizeBytes)

	// Check timeout
	require.True(t, ctrl.minTimeout >= ctrl.Timeout(), "expected timeout %v, got %v", ctrl.minTimeout, ctrl.Timeout())
}

func TestOnAckUnknownSeqNum(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Register the acknowledgement for packet with sequence number 1
	seqNum := uint16(1)
	ack := Ack{
		Delay:      150 * time.Millisecond,
		RTT:        300 * time.Millisecond,
		ReceivedAt: time.Now(),
	}

	err := ctrl.OnAck(seqNum, ack)
	require.ErrorIs(t, err, ErrUnknownSeqNum, "expected ErrUnknownSeqNum, got %v", err)
}

func TestOnLostPacketRetransmitting(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	initialMaxWindowSizeBytes := ctrl.minWindowSizeBytes * 10
	ctrl.maxWindowSizeBytes = initialMaxWindowSizeBytes

	// Register initial transmission
	seqNum := uint16(1)
	bytes := uint32(32)

	err := ctrl.OnTransmit(seqNum, Initial, bytes)
	require.NoError(t, err, "transmission registration failed")
	require.Equal(t, bytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", bytes, ctrl.windowSizeBytes)

	// Register packet loss with retransmission
	err = ctrl.OnLostPacket(seqNum, true)
	require.NoError(t, err, "lost packet registration failed")

	require.Equal(t, bytes, ctrl.windowSizeBytes,
		"expected window size %d, got %d", bytes, ctrl.windowSizeBytes)

	require.True(t, ctrl.maxWindowSizeBytes >= ctrl.minWindowSizeBytes,
		"max window size less than min window size")

	expectedMaxWindow := initialMaxWindowSizeBytes / 2
	require.Equal(t, expectedMaxWindow, ctrl.maxWindowSizeBytes,
		"expected max window size %d, got %d", expectedMaxWindow, ctrl.maxWindowSizeBytes)
}

func TestOnLostPacketUnknownSeqNum(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Set initial max window size
	initialMaxWindowSizeBytes := ctrl.minWindowSizeBytes * 10
	ctrl.maxWindowSizeBytes = initialMaxWindowSizeBytes

	// Try to register loss for unknown sequence number
	seqNum := uint16(1)
	err := ctrl.OnLostPacket(seqNum, false)
	// Check error
	require.ErrorIs(t, err, ErrUnknownSeqNum, "lost packet registration failed")

	// Verify window sizes unchanged
	require.Equal(t, uint32(0), ctrl.windowSizeBytes, "expected window size 0, got %d", ctrl.windowSizeBytes)
	require.Equal(t, initialMaxWindowSizeBytes, ctrl.maxWindowSizeBytes,
		"expected max window size %d, got %d", initialMaxWindowSizeBytes, ctrl.maxWindowSizeBytes)
}

func TestOnTimeout(t *testing.T) {
	ctrl := NewDefaultController(DefaultCtrlConfig())

	// Set initial max window size
	initialMaxWindowSizeBytes := ctrl.minWindowSizeBytes * 10
	ctrl.maxWindowSizeBytes = initialMaxWindowSizeBytes

	// Store initial timeout
	initialTimeout := ctrl.Timeout()

	// Register timeout
	err := ctrl.OnTimeout()
	require.NoError(t, err, "timeout registration failed")

	// Verify max window size was reset to minimum
	require.Equal(t, ctrl.minWindowSizeBytes, ctrl.maxWindowSizeBytes,
		"expected max window size %d, got %d", ctrl.minWindowSizeBytes, ctrl.maxWindowSizeBytes)

	// Verify timeout was doubled
	expectedTimeout := initialTimeout * 2
	require.Equal(t, expectedTimeout, ctrl.Timeout(),
		"expected timeout %v, got %v", expectedTimeout, ctrl.Timeout())
}

func TestOnTimeoutNotExceedMax(t *testing.T) {
	config := DefaultCtrlConfig()
	config.InitialTimeout = 2 * time.Second
	config.MaxTimeout = 3 * time.Second

	ctrl := NewDefaultController(config)

	// Register timeout
	err := ctrl.OnTimeout()
	require.NoError(t, err, "timeout registration failed")

	// Verify timeout is capped at max
	require.Equal(t, config.MaxTimeout, ctrl.Timeout(),
		"expected timeout %v, got %v", config.MaxTimeout, ctrl.Timeout())
}

func TestPush(t *testing.T) {
	window := 100 * time.Millisecond
	acc := NewDelayAccumulator(window)

	delay := 50 * time.Millisecond
	delayReceivedAt := time.Now()
	acc.Push(delay, delayReceivedAt)

	require.Equal(t, 1, acc.delays.Len(), "delay not pushed onto accumulator")

	item := (*acc.delays)[0]
	require.Equal(t, delay, item.Value,
		"expected delay %v, got %v", delay, item.Value)
	expectedDeadline := delayReceivedAt.Add(window)
	require.Equal(t, expectedDeadline, item.Deadline,
		"expected delay %v, got %v", expectedDeadline, item.Deadline)
}

func TestBaseDelay(t *testing.T) {
	window := 100 * time.Millisecond
	acc := NewDelayAccumulator(window)

	// Push delays in descending order
	delaySmall := 50 * time.Millisecond
	delaySmallReceivedAt := time.Now()
	acc.Push(delaySmall, delaySmallReceivedAt)

	delaySmaller := 25 * time.Millisecond
	delaySmallerReceivedAt := time.Now()
	acc.Push(delaySmaller, delaySmallerReceivedAt)

	delaySmallest := 5 * time.Millisecond
	delaySmallestReceivedAt := time.Now()
	acc.Push(delaySmallest, delaySmallestReceivedAt)

	delayExpired := 1 * time.Millisecond
	delayExpiredReceivedAt := time.Now().Add(-window)
	acc.Push(delayExpired, delayExpiredReceivedAt)

	// Check that all delays are present
	require.Equal(t, 4, acc.delays.Len(), "expected 4 delays, got %d", acc.delays.Len())

	// Get base delay
	baseDelay := acc.BaseDelay()
	require.Equal(t, delaySmallest, baseDelay, "expected base delay %v, got %v", delaySmallest, baseDelay)
	// Check that expired delay was removed
	require.Equal(t, 3, acc.delays.Len(), "expected 3 delays, got %d", acc.delays.Len())
}

func TestBaseDelayEmpty(t *testing.T) {
	window := time.Millisecond * 100
	acc := NewDelayAccumulator(window)
	baseDelay := acc.BaseDelay()
	require.Equal(t, time.Duration(0), baseDelay, "expected base delay 0, got %v", baseDelay)
}
