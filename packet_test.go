package utp_go

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

func (p *PacketHeaderV1) Generate(rand *rand.Rand, size int) reflect.Value {
	packetType := PacketType(rand.Intn(5))
	extension := byte(rand.Intn(256))
	header := &PacketHeaderV1{
		PacketType:    packetType,
		Version:       PROTOCOL_VERSION_ONE,
		Extension:     extension,
		ConnectionId:  uint16(rand.Intn(math.MaxUint16)),
		Timestamp:     int64(rand.Intn(math.MaxUint32)),
		TimestampDiff: uint32(rand.Intn(math.MaxUint32)),
		WndSize:       uint32(rand.Intn(math.MaxUint32)),
		SeqNum:        uint16(rand.Intn(math.MaxUint16)),
		AckNum:        uint16(rand.Intn(math.MaxUint16)),
	}
	return reflect.ValueOf(header)
}

func (a *SelectiveAck) Generate(rand *rand.Rand, size int) reflect.Value {
	bits := rand.Intn(size)
	acked := make([]bool, bits)
	for i := 0; i < bits; i++ {
		acked[i] = rand.Intn(2) == 1
	}
	//if len(acked) == 0 {
	//	var empty [32]bool
	//	acked = empty[:]
	//}
	return reflect.ValueOf(NewSelectiveAck(acked))
}

func TestPacketHeaderEncodeDecode(t *testing.T) {
	config := &quick.Config{
		MaxCount: 1000,
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	checkPacketHeader := func(header *PacketHeaderV1) bool {
		encoded := header.EncodeToBytes()
		if len(encoded) != 20 {
			return false
		}
		headerFromDecode, err := DecodePacketHeader(encoded)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(header, headerFromDecode)
	}
	if err := quick.Check(checkPacketHeader, config); err != nil {
		t.Error(err)
	}
}

func TestSelectiveAckEncodeDecode(t *testing.T) {
	config := &quick.Config{
		MaxCount: 1000,
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	checkSelective := func(ack *SelectiveAck) bool {
		encodedLen := ack.EncodedLen()
		encoded := ack.Encode()
		if len(encoded)%(SELECTIVE_ACK_BITS/8) != 0 {
			return false
		}
		if len(encoded) != encodedLen {
			return false
		}
		ackFromDecode, err := DecodeSelectiveAck(encoded)
		if err != nil {
			if encodedLen < 4 && errors.Is(err, ErrInsufficientSelectiveAckLen) {
				return true
			}
			t.Logf("expected err to be nil, got %v", err)
			return false
		}
		res := reflect.DeepEqual(ack, ackFromDecode)
		if !res {
			t.Logf("expected %v, got %v", ack, ackFromDecode)
		}
		return res
	}
	if err := quick.Check(checkSelective, config); err != nil {
		t.Error(err)
	}
}

func TestPacket(t *testing.T) {
	config := &quick.Config{
		MaxCount: 1000,
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	// Helper function to check property
	checkPacket := func(header *PacketHeaderV1, selectiveAck *SelectiveAck, payload []byte) bool {
		// Check empty payload
		if len(payload) == 0 {
			return true
		}

		// Handle selective ack
		if len(selectiveAck.acked) > 0 {
			header.Extension = 1
		} else {
			selectiveAck = nil
			header.Extension = 0
		}

		// Create packet
		packetInst := &Packet{
			Header: header,
			Eack:   selectiveAck,
			Body:   payload,
		}

		// Get encoded length
		encodedLen := packetInst.EncodedLen()

		// Encode packet
		encoded := packetInst.Encode()

		// Check length
		if len(encoded) != encodedLen {
			t.Errorf("encoded length mismatch: got %d, want %d", len(encoded), encodedLen)
			return false
		}

		// Decode packet

		decoded, err := DecodePacket(encoded)
		if err != nil {
			t.Errorf("failed to decode packet: %v", err)
			return false
		}

		// Compare packets
		return reflect.DeepEqual(decoded, packetInst)
	}

	if err := quick.Check(checkPacket, config); err != nil {
		t.Error(err)
	}
}
