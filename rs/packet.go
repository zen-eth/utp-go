package rs

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	PACKET_HEADER_LEN  = 20
	SELECTIVE_ACK_BITS = 32
	EXTENSION_TYPE_LEN = 1
	EXTENSION_LEN_LEN  = 1
)

const (
	Data PacketType = iota
	Fin
	State
	Reset
	Syn
)

var (
	ErrInvalidHeaderSize    = errors.New("invalid header size")
	ErrInvalidPacketVersion = errors.New("invalid packet version")
	ErrInvalidPacketType    = errors.New("invalid packet type")
	ErrInvalidExtensionType = errors.New("invalid extension type")
	ErrPacketTooShort       = errors.New("packet too short for selective ack extension")
)

type PacketType uint8

func (p PacketType) Check() error {
	if p > 4 || p < 0 {
		return ErrInvalidPacketType
	}
	return nil
}

type PacketHeader struct {
	PacketType   PacketType
	Version      uint8
	Extension    uint8
	ConnID       uint16
	TsMicros     uint32
	TsDiffMicros uint32
	WindowSize   uint32
	SeqNum       uint16
	AckNum       uint16
}

// Encode encodes the PacketHeader into a byte slice
func (h *PacketHeader) Encode() []byte {
	buf := new(bytes.Buffer)
	typeVersion := (uint8(h.PacketType) << 4) | h.Version
	buf.WriteByte(typeVersion)
	buf.WriteByte(h.Extension)
	binary.Write(buf, binary.BigEndian, h.ConnID)
	binary.Write(buf, binary.BigEndian, h.TsMicros)
	binary.Write(buf, binary.BigEndian, h.TsDiffMicros)
	binary.Write(buf, binary.BigEndian, h.WindowSize)
	binary.Write(buf, binary.BigEndian, h.SeqNum)
	binary.Write(buf, binary.BigEndian, h.AckNum)
	return buf.Bytes()
}

// DecodePacketHeader decodes a byte slice into a PacketHeader
func DecodePacketHeader(data []byte) (*PacketHeader, error) {
	if len(data) != PACKET_HEADER_LEN {
		return nil, errors.New("invalid packet header length")
	}
	h := &PacketHeader{}
	h.PacketType = PacketType(data[0] >> 4)
	h.Version = data[0] & 0x0F
	h.Extension = data[1]
	h.ConnID = binary.BigEndian.Uint16(data[2:4])
	h.TsMicros = binary.BigEndian.Uint32(data[4:8])
	h.TsDiffMicros = binary.BigEndian.Uint32(data[8:12])
	h.WindowSize = binary.BigEndian.Uint32(data[12:16])
	h.SeqNum = binary.BigEndian.Uint16(data[16:18])
	h.AckNum = binary.BigEndian.Uint16(data[18:20])
	return h, nil
}

// SelectiveAck represents a selective acknowledgment
type SelectiveAck struct {
	acked [][SELECTIVE_ACK_BITS]bool
}

func (s *SelectiveAck) Acked() []bool {
	result := make([]bool, 0, len(s.acked)*SELECTIVE_ACK_BITS)
	for _, boolArray := range s.acked {
		result = append(result, boolArray[:]...)
	}
	return result
}

// NewSelectiveAck creates a new SelectiveAck from a slice of booleans
func NewSelectiveAck(acked []bool) *SelectiveAck {
	chunks := len(acked) / SELECTIVE_ACK_BITS
	remainder := len(acked) % SELECTIVE_ACK_BITS

	ack := &SelectiveAck{acked: make([][SELECTIVE_ACK_BITS]bool, chunks)}
	for i := 0; i < chunks; i++ {
		var fragment [32]bool
		copy(fragment[:], acked[i*SELECTIVE_ACK_BITS:(i+1)*SELECTIVE_ACK_BITS])
		ack.acked[i] = fragment
	}

	if remainder > 0 {
		var fragment [32]bool
		copy(fragment[:], acked[chunks*SELECTIVE_ACK_BITS:])
		ack.acked = append(ack.acked, fragment)
	}

	return ack
}

// Encode encodes the SelectiveAck into a byte slice
func (s *SelectiveAck) Encode() []byte {
	var bitmask []byte
	for _, word := range s.acked {
		for i := 0; i < len(word); i += 8 {
			var byteVal uint8
			for j := 0; j < 8; j++ {
				if i+j < len(word) && word[i+j] {
					byteVal |= 1 << (7 - j)
				}
			}
			bitmask = append(bitmask, byteVal)
		}
	}
	return bitmask
}

// DecodeSelectiveAck decodes a byte slice into a SelectiveAck
func DecodeSelectiveAck(data []byte) (*SelectiveAck, error) {
	if len(data) < 4 {
		return nil, errors.New("insufficient length for selective ACK")
	}
	if len(data)%4 != 0 {
		return nil, errors.New("invalid length for selective ACK")
	}

	acked := make([][SELECTIVE_ACK_BITS]bool, len(data)/4)
	for i := 0; i < len(data); i += 4 {
		var tmp [SELECTIVE_ACK_BITS]bool
		for j := 0; j < 4; j++ {
			byteVal := data[i+j]
			for k := 0; k < 8; k++ {
				tmp[j*8+k] = (byteVal & (1 << (7 - k))) != 0
			}
		}
		acked[i/4] = tmp
	}

	return &SelectiveAck{acked: acked}, nil
}

type Packet struct {
	Header       PacketHeader
	SelectiveAck *SelectiveAck
	Payload      []byte
}

// Encode encodes the Packet into a byte slice
func (p *Packet) Encode() []byte {
	buf := new(bytes.Buffer)
	buf.Write(p.Header.Encode())
	if p.SelectiveAck != nil {
		ack := p.SelectiveAck.Encode()
		buf.WriteByte(uint8(0))
		buf.WriteByte(uint8(len(ack)))
		buf.Write(ack)
	}
	buf.Write(p.Payload)
	return buf.Bytes()
}

// DecodePacket decodes a byte slice into a Packet
func DecodePacket(data []byte) (*Packet, error) {
	if len(data) < PACKET_HEADER_LEN {
		return nil, errors.New("invalid packet length")
	}

	header, err := DecodePacketHeader(data[:PACKET_HEADER_LEN])
	if err != nil {
		return nil, err
	}

	// Decode extensions and payload
	// This is a simplified version and may need adjustments
	payload := data[PACKET_HEADER_LEN:]
	packet := &Packet{Header: *header, Payload: payload}

	return packet, nil
}
