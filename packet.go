package utp_go

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	MINIMAL_HEADER_SIZE                    = 20
	MINIMAL_HEADER_SIZE_WITH_SELECTIVE_ACK = 26
	PROTOCOL_VERSION                       = 1
	ZERO_MOMENT                            = time.Duration(0)
	ACKS_ARRAY_LENGTH                      = byte(4)
)

const (
	ST_DATA PacketType = iota
	ST_FIN
	ST_STATE
	ST_RESET
	ST_SYN
)

var (
	ErrInvalidHeaderSize    = errors.New("invalid header size")
	ErrInvalidPacketVersion = errors.New("invalid packet version")
	ErrInvalidPacketType    = errors.New("invalid packet type")
	ErrInvalidExtensionType = errors.New("invalid extension type")
	ErrPacketTooShort       = errors.New("packet too short for selective ack extension")
)

type PacketType byte

func (p PacketType) Check() error {
	if p > 4 || p < 0 {
		return ErrInvalidPacketType
	}
	return nil
}

type PacketHeaderV1 struct {
	PacketType    PacketType
	Version       byte
	Extension     byte
	ConnectionId  uint16
	Timestamp     uint32
	TimestampDiff uint32
	WndSize       uint32
	SeqNr         uint16
	AckNr         uint16
}

type SelectiveAckExtension [4]byte

type Packet struct {
	Header *PacketHeaderV1
	Eack   *SelectiveAckExtension
	Body   []byte
}

func (h *PacketHeaderV1) encodeTypeVer() byte {
	typeVer := byte(0)
	typeOrd := byte(h.PacketType)
	typeVer = (typeVer & 0xf0) | (h.Version & 0xf)
	typeVer = (typeVer & 0xf) | (typeOrd >> 4)
	return typeVer
}

func (h *PacketHeaderV1) EncodeToBytes() []byte {
	var b []byte
	b = append(b, h.encodeTypeVer(), h.Extension)         // 2
	b = binary.BigEndian.AppendUint16(b, h.ConnectionId)  // 2
	b = binary.BigEndian.AppendUint32(b, h.Timestamp)     // 4
	b = binary.BigEndian.AppendUint32(b, h.TimestampDiff) // 4
	b = binary.BigEndian.AppendUint32(b, h.WndSize)       // 4
	b = binary.BigEndian.AppendUint16(b, h.SeqNr)         // 2
	b = binary.BigEndian.AppendUint16(b, h.AckNr)         // 2
	return b
}

func (s *SelectiveAckExtension) EncodeToBytes() []byte {
	var b []byte
	b = append(b, ACKS_ARRAY_LENGTH)
	b = append(b, s[:]...)
	return b
}

func (p *Packet) EncodeToBytes() []byte {
	var b []byte
	b = append(b, p.Header.EncodeToBytes()...)
	if p.Eack != nil {
		b = append(b, p.Eack.EncodeToBytes()...)
	}
	if len(p.Body) > 0 {
		b = append(b, p.Body...)
	}
	return b
}

func (p *Packet) Decode(b []byte) error {
	receivedBytesLength := len(b)
	if receivedBytesLength < MINIMAL_HEADER_SIZE {
		return ErrInvalidHeaderSize
	}

	version := b[0] & 0xf
	if version != PROTOCOL_VERSION {
		return ErrInvalidPacketVersion
	}
	kind := PacketType(b[0] >> 4)
	if err := kind.Check(); err != nil {
		return err
	}
	extensionByte := b[1]
	if extensionByte != 0 && extensionByte != 1 {
		return ErrInvalidExtensionType
	}

	header := &PacketHeaderV1{
		PacketType:    kind,
		Version:       version,
		Extension:     extensionByte,
		ConnectionId:  binary.BigEndian.Uint16(b[2:4]),
		Timestamp:     binary.BigEndian.Uint32(b[4:8]),
		TimestampDiff: binary.BigEndian.Uint32(b[8:12]),
		WndSize:       binary.BigEndian.Uint32(b[12:16]),
		SeqNr:         binary.BigEndian.Uint16(b[16:18]),
		AckNr:         binary.BigEndian.Uint16(b[18:20]),
	}

	var body []byte
	if extensionByte == 0 {
		if receivedBytesLength == MINIMAL_HEADER_SIZE {
			body = []byte{}
		} else {
			body = b[MINIMAL_HEADER_SIZE:]
		}
		p.Header = header
		p.Eack = nil
		p.Body = body
		return nil
	}
	if receivedBytesLength < MINIMAL_HEADER_SIZE_WITH_SELECTIVE_ACK {
		return ErrPacketTooShort
	}
	nextExtension := b[20]
	extLength := b[21]

	if nextExtension != 0 || extLength != 4 {
		return fmt.Errorf("bad format of selective ack extension: extension=%d, len=%d", nextExtension, extLength)
	}

	var extension SelectiveAckExtension
	copy(extension[:], b[22:26])

	if receivedBytesLength == MINIMAL_HEADER_SIZE_WITH_SELECTIVE_ACK {
		body = []byte{}
	} else {
		body = b[MINIMAL_HEADER_SIZE_WITH_SELECTIVE_ACK:]
	}
	p.Header = header
	p.Eack = &extension
	p.Body = body
	return nil
}

func ModifyTimeStampAndAckNr(packetBytes []byte, newTimestamp uint32, newAckNr uint16) error {
	if len(packetBytes) < MINIMAL_HEADER_SIZE {
		return ErrPacketTooShort
	}

	timestampBytes := binary.BigEndian.AppendUint32([]byte{}, newTimestamp)
	copy(packetBytes[4:8], timestampBytes)
	newAckNrBytes := binary.BigEndian.AppendUint16([]byte{}, newAckNr)
	copy(packetBytes[18:20], newAckNrBytes)
	return nil
}

func SynPacket(seqNr uint16, recvId uint16, bufferSize uint32) *Packet {
	header := &PacketHeaderV1{
		PacketType:    ST_SYN,
		Version:       PROTOCOL_VERSION,
		Extension:     0,
		ConnectionId:  recvId,
		Timestamp:     uint32(time.Now().UnixMicro()),
		TimestampDiff: 0,
		WndSize:       bufferSize,
		SeqNr:         seqNr,
		AckNr:         0,
	}
	return &Packet{
		Header: header,
		Eack:   nil,
		Body:   make([]byte, 0),
	}
}

func AckPacket(seqNr uint16, sendId uint16, ackNr uint16, bufferSize uint32, timestampDiff uint32, acksBitmask *[4]byte) *Packet {
	var extensionByte byte
	var extension *SelectiveAckExtension
	if acksBitmask != nil {
		extensionByte = 1
		*extension = *acksBitmask
	}

	header := &PacketHeaderV1{
		PacketType:    ST_STATE,
		Version:       PROTOCOL_VERSION,
		Extension:     extensionByte,
		ConnectionId:  sendId,
		Timestamp:     uint32(time.Now().UnixMicro()),
		TimestampDiff: timestampDiff,
		WndSize:       bufferSize,
		SeqNr:         seqNr,
	}
	return &Packet{
		Header: header,
		Eack:   extension,
		Body:   make([]byte, 0),
	}
}

func DataPacket(seqNr uint16, sendId uint16, ackNr uint16, bufferSize uint32, payload []byte, timestampDiff uint32) *Packet {
	header := &PacketHeaderV1{
		PacketType:    ST_DATA,
		Version:       PROTOCOL_VERSION,
		Extension:     0,
		ConnectionId:  sendId,
		Timestamp:     uint32(time.Now().UnixMicro()),
		TimestampDiff: timestampDiff,
		WndSize:       bufferSize,
		SeqNr:         seqNr,
		AckNr:         ackNr,
	}
	return &Packet{
		Header: header,
		Eack:   nil,
		Body:   payload,
	}
}

func ResetPacket(seqNr uint16, sendId uint16, ackNr uint16) *Packet {
	header := &PacketHeaderV1{
		PacketType:    ST_RESET,
		Version:       PROTOCOL_VERSION,
		Extension:     0,
		ConnectionId:  sendId,
		Timestamp:     uint32(time.Now().UnixMicro()),
		TimestampDiff: 0,
		WndSize:       0,
		SeqNr:         seqNr,
		AckNr:         ackNr,
	}
	return &Packet{
		Header: header,
		Eack:   nil,
		Body:   make([]byte, 0),
	}
}

func FinPacket(seqNr uint16, sendId uint16, ackNr uint16, bufferSize uint32, timestampDiff uint32) *Packet {
	header := &PacketHeaderV1{
		PacketType:    ST_FIN,
		Version:       PROTOCOL_VERSION,
		Extension:     0,
		ConnectionId:  sendId,
		Timestamp:     uint32(time.Now().UnixMicro()),
		TimestampDiff: timestampDiff,
		WndSize:       bufferSize,
		SeqNr:         seqNr,
		AckNr:         ackNr,
	}
	return &Packet{
		Header: header,
		Eack:   nil,
		Body:   make([]byte, 0),
	}
}
