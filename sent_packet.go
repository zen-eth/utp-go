package utp_go

import (
	"errors"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/btree"
)

const LossThreshold = 3

var ErrInvalidAckNum = errors.New("invalid ack number")
var ErrNoneAckNum = errors.New("none ack number")
var ErrCannotFindLostPacket = errors.New("cannot mark unsent packet lost")
var ErrSentPacketMarkLost = errors.New("lost packet was previously sent")

type LostPacket struct {
	SeqNum     uint16
	PacketType PacketType
	Data       []byte
}

type sentPacket struct {
	seqNum         uint16
	packetType     PacketType
	data           []byte
	transmission   time.Time
	retransmission time.Time
	acks           []time.Time
}

func (s *sentPacket) rtt(now time.Time) time.Duration {
	var lastTransmission time.Time
	if s.retransmission == s.transmission {
		lastTransmission = s.transmission
	} else {
		lastTransmission = s.retransmission
	}
	return now.Sub(lastTransmission)
}

type LostPacketSeqNums []uint16

func (l LostPacketSeqNums) Remove(seq uint16) LostPacketSeqNums {
	var n []uint16
	for i, seqNum := range l {
		if seqNum == seq {
			n = append((l)[:i], (l)[i+1:]...)
			break
		}
	}
	return n
}

type sentPackets struct {
	logger         log.Logger
	packets        []*sentPacket
	initSeqNum     uint16
	lostPackets    *btree.BTreeG[uint16]
	congestionCtrl Controller
}

func newSentPackets(initSeqNum uint16, congestionCtrl Controller, logger log.Logger) *sentPackets {
	return &sentPackets{
		logger:         logger,
		packets:        make([]*sentPacket, 0),
		initSeqNum:     initSeqNum,
		lostPackets:    btree.NewOrderedG[uint16](2),
		congestionCtrl: congestionCtrl,
	}
}

func newSentPacketsWithoutLogger(initSeqNum uint16, congestionCtrl Controller) *sentPackets {
	return newSentPackets(initSeqNum, congestionCtrl, nil)
}

func (s *sentPackets) OnTimeout() {
	s.congestionCtrl.OnTimeout()
}

func (s *sentPackets) NextSeqNum() uint16 {
	return s.initSeqNum + uint16(len(s.packets)) + uint16(1)
}

func (s *sentPackets) AckNum() uint16 {
	num, isNone := s.LastAckNum()
	if isNone {
		return 0
	}
	return num
}

func (s *sentPackets) SeqNumRange() *circularRangeInclusive {
	end := s.NextSeqNum() - uint16(1)
	return newCircularRangeInclusive(s.initSeqNum, end)
}

func (s *sentPackets) Timeout() time.Duration {
	return s.congestionCtrl.Timeout()
}

func (s *sentPackets) Window() uint32 {
	return s.congestionCtrl.BytesAvailableInWindow()
}

func (s *sentPackets) HasUnackedPackets() bool {
	_, err := s.FirstUnackedSeqNum()
	return err == nil
}

func (s *sentPackets) HasLostPackets() bool {
	return s.lostPackets.Len() != 0
}

func (s *sentPackets) LostPackets() []*LostPacket {
	var result []*LostPacket

	s.lostPackets.Ascend(func(seqNum uint16) bool {
		index := s.SeqNumIndex(seqNum)
		packetInst := s.packets[index] // We can directly access since we know lost packets must exist
		lostPacket := &LostPacket{
			packetInst.seqNum,     // uint16
			packetInst.packetType, // PacketType
			packetInst.data,       // []byte or nil
		}

		result = append(result, lostPacket)
		return true
	})

	return result
}

func (s *sentPackets) OnTransmit(
	seqNum uint16,
	packetType PacketType,
	data []byte,
	dataLen uint32,
	now time.Time,
) {
	index := s.SeqNumIndex(seqNum)
	isRetransmission := index < len(s.packets)

	// Check for out of order transmit
	if index > len(s.packets) {
		panic("out of order transmit")
	}

	// Check window size for new transmissions
	if !isRetransmission && dataLen > s.Window() {
		panic("transmit exceeds available send window")
	}

	if index < len(s.packets) {
		// Update existing packet
		s.packets[index].retransmission = now
	} else {
		// Create new packet
		sent := &sentPacket{
			seqNum:         seqNum,
			packetType:     packetType,
			data:           data,
			transmission:   now,
			retransmission: now,
			acks:           make([]time.Time, 0),
		}
		s.packets = append(s.packets, sent)
	}

	var transmit Transmit
	if isRetransmission {
		transmit = Retransmission
	} else {
		transmit = Initial
	}

	if err := s.congestionCtrl.OnTransmit(seqNum, transmit, dataLen); err != nil {
		panic(err)
	}
}

func (s *sentPackets) onAck(
	ackNum uint16,
	selectiveAck *SelectiveAck,
	delay time.Duration,
	now time.Time,
) (*circularRangeInclusive, []uint16, error) {
	// Check if ack number is in valid range
	seqRange := s.SeqNumRange()
	if !seqRange.Contains(ackNum) {
		if len(s.packets) != 0 && seqRange.end == seqRange.start {
			seqRange = newCircularRangeInclusive(seqRange.start, seqRange.end-1)
			if !seqRange.Contains(ackNum) {
				return nil, nil, ErrInvalidAckNum
			}
		} else {
			return nil, nil, ErrInvalidAckNum
		}
	}

	// Do not ACK if ACK num corresponds to initial packet
	if ackNum != seqRange.Start() {
		if err := s.OnAckNum(ackNum, selectiveAck, delay, now); err != nil {
			return nil, nil, err
		}
	}

	// Mark all packets up to ackNum as acknowledged
	fullAcked := newCircularRangeInclusive(seqRange.Start(), ackNum)

	if selectiveAck != nil {
		selectedAcks := make([]uint16, 0)
		acked := selectiveAck.Acked()
		for i, isAcked := range acked {
			if isAcked {
				// Double wrapping addition for uint16
				indexNum := ackNum + uint16(2) + uint16(i)
				selectedAcks = append(selectedAcks, indexNum)
			}
		}
		return fullAcked, selectedAcks, nil
	}
	return fullAcked, nil, nil
}

func (s *sentPackets) OnAckNum(
	ackNum uint16,
	selectiveAck *SelectiveAck,
	delay time.Duration,
	now time.Time,
) error {
	var err error
	if selectiveAck != nil {
		err = s.OnSelectiveAck(ackNum, selectiveAck, delay, now)
	} else {
		err = s.Ack(ackNum, delay, now)
	}
	if err != nil {
		return err
	}

	firstUnacked, err := s.FirstUnackedSeqNum()
	if err != nil {
		return err
	}

	// An ACK for ackNum implicitly ACKs all sequence numbers that precede ackNum
	// Account for any preceding innerMap packets
	if err = s.AckPriorUnacked(ackNum, firstUnacked, delay, now); err != nil {
		return err
	}

	// Account for (newly) lost packets
	losts := s.DetectLostPackets(firstUnacked)
	for _, packetInst := range losts {
		s.lostPackets.ReplaceOrInsert(packetInst)
		_ = s.OnLost(packetInst, true)
	}
	return nil
}

func (s *sentPackets) OnSelectiveAck(
	ackNum uint16,
	selectiveAck *SelectiveAck,
	delay time.Duration,
	now time.Time,
) error {
	if err := s.Ack(ackNum, delay, now); err != nil {
		return err
	}

	seqRange := s.SeqNumRange()

	// The first bit of the selective ACK corresponds to ackNum + 2,
	// where ackNum + 1 is assumed to have been dropped
	sackNum := ackNum + 2

	for _, ack := range selectiveAck.Acked() {
		// Break once we exhaust all sent sequence numbers
		// The selective ACK length is a multiple of 32, so it may be padded
		if !seqRange.Contains(sackNum) {
			break
		}

		if ack {
			if err := s.Ack(sackNum, delay, now); err != nil {
				return err
			}
		}

		sackNum += uint16(1) // wrapping addition for uint16
	}
	return nil
}

func (s *sentPackets) DetectLostPackets(firstUnacked uint16) []uint16 {
	var lost []uint16
	acked := 0

	startIndex := s.SeqNumIndex(firstUnacked)
	packets := s.packets[startIndex:]

	// Iterate in reverse order
	for i := len(packets) - 1; i >= 0; i-- {
		packetInst := packets[i]

		if len(packetInst.acks) == 0 && acked >= LossThreshold {
			lost = append(lost, packetInst.seqNum)
		}
		if len(packetInst.acks) > 0 {
			acked++
		}
	}

	return lost
}

func (s *sentPackets) Ack(seqNum uint16, delay time.Duration, now time.Time) error {
	index := s.SeqNumIndex(seqNum)
	packetInst := s.packets[index]
	ack := Ack{
		Delay:      delay,
		RTT:        packetInst.rtt(now),
		ReceivedAt: now,
	}

	if err := s.congestionCtrl.OnAck(packetInst.seqNum, ack); err != nil {
		return err
	}
	if s.logger != nil && s.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		log.Trace("record Acks", "seqNum", packetInst.seqNum, "acks.len", len(packetInst.acks)+1)
	}
	if len(packetInst.acks) == 0 {
		packetInst.acks = append(packetInst.acks, now)
		s.lostPackets.Delete(packetInst.seqNum)
	}
	return nil
}

func (s *sentPackets) AckPriorUnacked(seqNum uint16, firstUnacked uint16, delay time.Duration, now time.Time) error {
	start := s.SeqNumIndex(firstUnacked)
	end := s.SeqNumIndex(seqNum)
	if start >= end {
		return nil
	}
	if s.logger != nil && s.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		s.logger.Trace("AckPriorUnacked", "sentPackets.len", len(s.packets), "start", start, "end", end)
	}
	for _, packetInst := range s.packets[start:end] {
		if s.logger != nil && s.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			s.logger.Trace("record Ack", "seqNum", packetInst.seqNum)
		}
		if err := s.Ack(packetInst.seqNum, delay, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *sentPackets) LastAckNum() (uint16, bool) {
	if len(s.packets) == 0 {
		return 0, true
	}
	var num uint16
	none := true
	for _, packetInst := range s.packets {
		if len(packetInst.acks) != 0 {
			num = packetInst.seqNum
			none = false
		} else {
			break
		}
	}
	return num, none
}

func (s *sentPackets) OnLost(seqNum uint16, retransmitting bool) error {
	if !s.SeqNumRange().Contains(seqNum) {
		return ErrCannotFindLostPacket
	}

	err := s.congestionCtrl.OnLostPacket(seqNum, retransmitting)
	if err != nil {
		return ErrSentPacketMarkLost
	}
	return nil
}

func (s *sentPackets) SeqNumIndex(seqNum uint16) int {
	// The first sequence number is equal to `s.initSeqNum + uint16(1)`.
	if seqNum > s.initSeqNum {
		return int(seqNum - s.initSeqNum - uint16(1))
	} else {
		return int(math.MaxUint16 - s.initSeqNum + seqNum)
	}
}

func (s *sentPackets) FirstUnackedSeqNum() (uint16, error) {
	if len(s.packets) == 0 {
		return 0, ErrNoneAckNum
	}

	var seqNum uint16
	lastAckNum, isNone := s.LastAckNum()
	if s.logger != nil && s.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		s.logger.Trace("get last innerMap num",
			"lastAckNum", lastAckNum, "isNone", isNone)
	}
	const one = uint16(1)
	if isNone {
		seqNum = s.initSeqNum + one
	} else {
		if s.packets[len(s.packets)-1].seqNum == lastAckNum {
			return 0, ErrNoneAckNum
		}
		seqNum = lastAckNum + 1 // wrapping addition for uint16
	}

	return seqNum, nil
}
