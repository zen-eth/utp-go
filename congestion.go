package utp_go

import (
	"container/heap"
	"errors"
	"math"
	"sync"
	"time"
)

const (
	defaultTargetMicros       = 100000 * time.Microsecond
	defaultInitialTimeout     = time.Second
	defaultMinTimeout         = 500 * time.Millisecond
	defaultMaxTimeout         = 60 * time.Second
	defaultMaxPacketSizeBytes = 1024
	defaultGain               = 1.0
	defaultDelayWindow        = 120 * time.Second
)

const (
	Initial = iota
	Retransmission
)

type Transmit int

type packet struct {
	SizeBytes        uint32
	NumTransmissions uint32
	Acked            bool
}

type Ack struct {
	Delay      time.Duration
	RTT        time.Duration
	ReceivedAt time.Time
}

var (
	ErrInsufficientWindowSize = errors.New("insufficient window size")
	ErrUnknownSeqNum          = errors.New("unknown sequence number")
	ErrDuplicateTransmission  = errors.New("duplicate transmission")
)

type ctrlConfig struct {
	TargetDelayMicros     uint32
	InitialTimeout        time.Duration
	MinTimeout            time.Duration
	MaxTimeout            time.Duration
	MaxPacketSizeBytes    uint32
	MaxWindowSizeIncBytes uint32
	Gain                  float32
	DelayWindow           time.Duration
	WindowSize            uint32
}

func defaultCtrlConfig() *ctrlConfig {
	return &ctrlConfig{
		TargetDelayMicros:     uint32(defaultTargetMicros.Microseconds()),
		InitialTimeout:        defaultInitialTimeout,
		MinTimeout:            defaultMinTimeout,
		MaxTimeout:            defaultMaxTimeout,
		MaxPacketSizeBytes:    defaultMaxPacketSizeBytes,
		MaxWindowSizeIncBytes: defaultMaxPacketSizeBytes,
		Gain:                  defaultGain,
		DelayWindow:           defaultDelayWindow,
	}
}

type Controller interface {
	OnTransmit(seqNum uint16, transmit Transmit, dataLen uint32) error
	OnAck(seqNum uint16, ack Ack) error
	OnLostPacket(seqNum uint16, retransmitting bool) error
	OnTimeout()
	Timeout() time.Duration
	BytesAvailableInWindow() uint32
}

type defaultController struct {
	targetDelayMicros     uint32
	timeout               time.Duration
	minTimeout            time.Duration
	maxTimeout            time.Duration
	windowSizeBytes       uint32
	maxWindowSizeBytes    uint32
	minWindowSizeBytes    uint32
	maxWindowSizeIncBytes uint32
	gain                  float32
	rtt                   time.Duration
	rttVarianceMicros     int64
	transmissions         map[uint16]*packet
	delayAcc              *delayAccumulator
	mu                    sync.Mutex
}

func newDefaultController(config *ctrlConfig) *defaultController {
	return &defaultController{
		targetDelayMicros:     config.TargetDelayMicros,
		timeout:               config.InitialTimeout,
		minTimeout:            config.MinTimeout,
		maxTimeout:            config.MaxTimeout,
		windowSizeBytes:       0,
		maxWindowSizeBytes:    2 * config.MaxPacketSizeBytes,
		minWindowSizeBytes:    2 * config.MaxPacketSizeBytes,
		maxWindowSizeIncBytes: config.MaxWindowSizeIncBytes,
		gain:                  config.Gain,
		rtt:                   0,
		rttVarianceMicros:     0,
		transmissions:         make(map[uint16]*packet),
		delayAcc:              newDelayAccumulator(config.DelayWindow),
	}
}

func (c *defaultController) Timeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timeout
}

func (c *defaultController) BytesAvailableInWindow() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxWindowSizeBytes > c.windowSizeBytes {
		return c.maxWindowSizeBytes - c.windowSizeBytes
	}
	return 0
}

func (c *defaultController) OnTransmit(seqNum uint16, transmission Transmit, dataLen uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var packetInst *packet
	if transmission == Initial {
		if _, exists := c.transmissions[seqNum]; exists {
			return ErrDuplicateTransmission
		}
		packetInst = &packet{
			SizeBytes:        dataLen,
			NumTransmissions: 1,
			Acked:            false,
		}
		c.transmissions[seqNum] = packetInst
	} else {
		var exists bool
		packetInst, exists = c.transmissions[seqNum]
		if !exists {
			return ErrUnknownSeqNum
		}
		packetInst.NumTransmissions++
	}
	if packetInst.NumTransmissions == 1 {
		if c.windowSizeBytes+packetInst.SizeBytes > c.maxWindowSizeBytes {
			return ErrInsufficientWindowSize
		}
		c.windowSizeBytes += packetInst.SizeBytes
	}

	return nil
}

func (c *defaultController) OnAck(seqNum uint16, ack Ack) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	packetInst, exists := c.transmissions[seqNum]
	if !exists {
		return ErrUnknownSeqNum
	}

	if packetInst.Acked {
		return nil
	}
	packetInst.Acked = true
	c.transmissions[seqNum] = packetInst

	c.delayAcc.Push(ack.Delay, ack.ReceivedAt)

	baseDelayMicros := uint32(c.delayAcc.BaseDelay().Microseconds())
	packetDelayMicros := uint32(ack.Delay.Microseconds())
	maxWindowSizeAdjustment := computeMaxWindowSizeAdjustment(
		c.targetDelayMicros,
		baseDelayMicros,
		packetDelayMicros,
		c.windowSizeBytes,
		packetInst.SizeBytes,
		c.maxWindowSizeIncBytes,
		c.gain,
	)
	c.applyMaxWindowSizeAdjustment(maxWindowSizeAdjustment)

	c.windowSizeBytes -= packetInst.SizeBytes

	if packetInst.NumTransmissions == 1 {
		rttVarAdjustment := computeRTTVarianceAdjustment(
			c.rtt.Microseconds(),
			c.rttVarianceMicros,
			ack.RTT.Microseconds(),
		)
		c.rttVarianceMicros = maxInt64(0, c.rttVarianceMicros+rttVarAdjustment)

		rttAdjustment := computeRTTAdjustment(c.rtt.Microseconds(), ack.RTT.Microseconds())

		c.rtt = time.Duration(maxInt64(c.rtt.Milliseconds()+rttAdjustment, 0))

		c.applyTimeoutAdjustment()
	}

	return nil
}

func (c *defaultController) OnLostPacket(seqNum uint16, retransmitting bool) error {
	packetInst, exists := c.transmissions[seqNum]
	if !exists {
		return ErrUnknownSeqNum
	}

	c.maxWindowSizeBytes = uint32(math.Max(float64(c.maxWindowSizeBytes/2), float64(c.minWindowSizeBytes)))

	if !retransmitting {
		c.windowSizeBytes -= packetInst.SizeBytes
	}

	return nil
}

func (c *defaultController) OnTimeout() {
	c.maxWindowSizeBytes = c.minWindowSizeBytes
	c.timeout = time.Duration(math.Min(float64(c.timeout*2), float64(c.maxTimeout)))
}

// applyMaxWindowSizeAdjustment adjusts the maximum window size based on the given adjustment.
func (c *defaultController) applyMaxWindowSizeAdjustment(adjustment int64) {
	// Apply the adjustment.
	adjMaxWindowSizeBytes := int64(c.maxWindowSizeBytes) + adjustment

	// The maximum congestion window must not fall below the minimum.
	if adjMaxWindowSizeBytes < int64(c.minWindowSizeBytes) {
		adjMaxWindowSizeBytes = int64(c.minWindowSizeBytes)
	}

	// The maximum congestion window cannot increase by more than the configured maximum increment.
	c.maxWindowSizeBytes = uint32(math.Min(
		float64(adjMaxWindowSizeBytes),
		float64(c.maxWindowSizeBytes)+float64(c.maxWindowSizeIncBytes),
	))
}

func (c *defaultController) applyTimeoutAdjustment() {
	c.timeout = time.Duration(math.Max(float64(c.rtt+time.Duration(c.rttVarianceMicros*4)), float64(c.minTimeout)))
	c.timeout = time.Duration(math.Min(float64(c.timeout), float64(c.maxTimeout)))
}

// computeMaxWindowSizeAdjustment returns the adjustment in bytes to the maximum window (i.e. congestion window) size
// based on the delta between the packet delay and the target delay and on the portion of the total
// in-flight bytes that the packet corresponds to.
func computeMaxWindowSizeAdjustment(
	targetDelayMicros uint32,
	baseDelayMicros uint32,
	packetDelayMicros uint32,
	windowSizeBytes uint32,
	packetSizeBytes uint32,
	maxWindowSizeIncBytes uint32,
	gain float32,
) int64 {
	// Adjust the delay based on the base delay.
	delayMicros := int64(packetDelayMicros) - int64(baseDelayMicros)

	offTargetMicros := int64(targetDelayMicros) - delayMicros
	delayFactor := float64(offTargetMicros) / float64(targetDelayMicros)
	windowFactor := float64(packetSizeBytes) / float64(windowSizeBytes)

	scaledGain := float64(gain) * float64(maxWindowSizeIncBytes) * delayFactor * windowFactor

	return int64(scaledGain)
}

// computeRTTAdjustment returns the adjustment to the round trip time (RTT) estimate in microseconds
// based on the packet RTT and the current RTT estimate.
func computeRTTAdjustment(rttMicros, packetRTTMicros int64) int64 {
	return int64((float64(packetRTTMicros) - float64(rttMicros)) / 8.0)
}

// computeRTTVarianceAdjustment returns the adjustment to round trip time (RTT) variance in microseconds
// based on the packet RTT, current RTT estimate, and current RTT variance.
func computeRTTVarianceAdjustment(rttMicros, rttVarianceMicros, packetRTTMicros int64) int64 {
	absDeltaMicros := math.Abs(float64(rttMicros - packetRTTMicros))
	return int64((absDeltaMicros - float64(rttVarianceMicros)) / 4.0)
}

// Additional methods for handling acknowledgments, lost packets, and timeouts would follow...

type delay struct {
	Value    time.Duration
	Deadline time.Time
}

type delayAccumulator struct {
	delays *delayHeap
	window time.Duration
}

func newDelayAccumulator(window time.Duration) *delayAccumulator {
	return &delayAccumulator{
		delays: &delayHeap{},
		window: window,
	}
}

func (da *delayAccumulator) Push(delayTime time.Duration, receivedAt time.Time) {
	heap.Push(da.delays, delay{
		Value:    delayTime,
		Deadline: receivedAt.Add(da.window),
	})
}

func (da *delayAccumulator) BaseDelay() time.Duration {
	now := time.Now()
	for da.delays.Len() > 0 {
		min := (*da.delays)[0]
		if now.After(min.Deadline) {
			heap.Pop(da.delays)
		} else {
			return min.Value
		}
	}
	return time.Duration(0)
}

type delayHeap []delay

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].Value < h[j].Value }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *delayHeap) Push(x interface{}) {
	*h = append(*h, x.(delay))
}

func (h *delayHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	x := old[n]
	*h = old[:n]
	return x
}
