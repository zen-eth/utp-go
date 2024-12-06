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
	PROTOCOL_VERSION_ONE                   = 1
	ZERO_MOMENT                            = time.Duration(0)
	ACKS_ARRAY_LENGTH                      = byte(4)
	PACKET_HEADER_LEN                      = 20
	SELECTIVE_ACK_BITS                     = 32
	EXTENSION_TYPE_LEN                     = 1
	EXTENSION_LEN_LEN                      = 1
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
	Timestamp     int64
	TimestampDiff uint32
	WndSize       uint32
	SeqNum        uint16
	AckNum        uint16
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
	b = append(b, h.encodeTypeVer(), h.Extension)             // 2
	b = binary.BigEndian.AppendUint16(b, h.ConnectionId)      // 2
	b = binary.BigEndian.AppendUint32(b, uint32(h.Timestamp)) // 4
	b = binary.BigEndian.AppendUint32(b, h.TimestampDiff)     // 4
	b = binary.BigEndian.AppendUint32(b, h.WndSize)           // 4
	b = binary.BigEndian.AppendUint16(b, h.SeqNum)            // 2
	b = binary.BigEndian.AppendUint16(b, h.AckNum)            // 2
	return b
}

func DecodePacketHeader(value []byte) (*PacketHeaderV1, error) {
	if len(value) < MINIMAL_HEADER_SIZE {
		return nil, ErrInvalidHeaderSize
	}

	packetType := value[0] >> 4
	packetTypeVal := PacketType(binary.BigEndian.Uint16([]byte{packetType}))

	version := value[0] & 0x0F
	versionVal := uint8(binary.BigEndian.Uint16([]byte{version}))

	extension := byte(binary.BigEndian.Uint16(value[1:2]))

	connID := binary.BigEndian.Uint16(value[2:4])
	tsMicros := binary.BigEndian.Uint32(value[4:8])
	tsDiffMicros := binary.BigEndian.Uint32(value[8:12])
	windowSize := binary.BigEndian.Uint32(value[12:16])
	seqNum := binary.BigEndian.Uint16(value[16:18])
	ackNum := binary.BigEndian.Uint16(value[18:20])

	return &PacketHeaderV1{
		PacketType:    packetTypeVal,
		Version:       versionVal,
		Extension:     extension,
		ConnectionId:  connID,
		Timestamp:     int64(tsMicros),
		TimestampDiff: tsDiffMicros,
		WndSize:       windowSize,
		SeqNum:        seqNum,
		AckNum:        ackNum,
	}, nil
}

// SelectiveAck represents a selective acknowledgment
type SelectiveAck struct {
	acked [][SELECTIVE_ACK_BITS]bool
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

func (s *SelectiveAck) EncodedLen() int {
	return (len(s.acked) * SELECTIVE_ACK_BITS) / 8
}

func (s *SelectiveAck) Acked() []bool {
	result := make([]bool, 0, len(s.acked)*SELECTIVE_ACK_BITS)
	for _, boolArray := range s.acked {
		result = append(result, boolArray[:]...)
	}
	return result
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

type SelectiveAckExtension [4]byte

type Packet struct {
	Header *PacketHeaderV1
	Eack   *SelectiveAck
	Body   []byte
}

func (p *Packet) EncodedLen() int {
	length := MINIMAL_HEADER_SIZE

	if p.Eack != nil {
		length += p.Eack.EncodedLen() + EXTENSION_TYPE_LEN + EXTENSION_LEN_LEN
	}

	length += len(p.Body)

	return length
}

func (p *Packet) Encode() []byte {
	bytes := make([]byte, 0)

	// Encode header
	bytes = append(bytes, p.Header.EncodeToBytes()...)

	// Encode selective ack if present
	if p.Eack != nil {
		ack := p.Eack.Encode()
		bytes = append(bytes, byte(0), byte(len(ack)))
		bytes = append(bytes, ack...)
	}

	// Append payload
	bytes = append(bytes, p.Body...)

	return bytes
}

func (p *Packet) Decode(b []byte) error {
	receivedBytesLength := len(b)
	if receivedBytesLength < MINIMAL_HEADER_SIZE {
		return ErrInvalidHeaderSize
	}

	version := b[0] & 0xf
	if version != PROTOCOL_VERSION_ONE {
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
		Timestamp:     int64(binary.BigEndian.Uint32(b[4:8])),
		TimestampDiff: binary.BigEndian.Uint32(b[8:12]),
		WndSize:       binary.BigEndian.Uint32(b[12:16]),
		SeqNum:        binary.BigEndian.Uint16(b[16:18]),
		AckNum:        binary.BigEndian.Uint16(b[18:20]),
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

	extension, err := DecodeSelectiveAck(b[22:26])
	if err != nil {
		return fmt.Errorf("invalid length of selective ack extension")
	}

	if receivedBytesLength == MINIMAL_HEADER_SIZE_WITH_SELECTIVE_ACK {
		body = []byte{}
	} else {
		body = b[MINIMAL_HEADER_SIZE_WITH_SELECTIVE_ACK:]
	}
	p.Header = header
	p.Eack = extension
	p.Body = body
	return nil
}

type PacketBuilder struct {
	packetType   PacketType
	connID       uint16
	tsMicros     uint32
	tsDiffMicros uint32
	windowSize   uint32
	seqNum       uint16
	ackNum       uint16
	selectiveAck *SelectiveAck
	payload      []byte
}

func NewPacketBuilder(
	packetType PacketType,
	connID uint16,
	tsMicros uint32,
	windowSize uint32,
	seqNum uint16,
) *PacketBuilder {
	return &PacketBuilder{
		packetType:   packetType,
		connID:       connID,
		tsMicros:     tsMicros,
		tsDiffMicros: 0,
		windowSize:   windowSize,
		seqNum:       seqNum,
		ackNum:       0,
	}
}

func (b *PacketBuilder) WithTsMicros(tsMicros uint32) *PacketBuilder {
	b.tsMicros = tsMicros
	return b
}

func (b *PacketBuilder) WithTsDiffMicros(tsDiffMicros uint32) *PacketBuilder {
	b.tsDiffMicros = tsDiffMicros
	return b
}

func (b *PacketBuilder) WithWindowSize(windowSize uint32) *PacketBuilder {
	b.windowSize = windowSize
	return b
}

func (b *PacketBuilder) WithAckNum(ackNum uint16) *PacketBuilder {
	b.ackNum = ackNum
	return b
}

func (b *PacketBuilder) WithSelectiveAck(selectiveAck *SelectiveAck) *PacketBuilder {
	b.selectiveAck = selectiveAck
	return b
}

func (b *PacketBuilder) WithPayload(payload []byte) *PacketBuilder {
	b.payload = payload
	return b
}

func (b *PacketBuilder) Build() *Packet {
	var headerExtension byte
	if b.selectiveAck != nil {
		headerExtension = 1
	}

	var payload []byte
	if b.payload != nil {
		payload = b.payload
	}

	return &Packet{
		Header: &PacketHeaderV1{
			PacketType:    b.packetType,
			Version:       PROTOCOL_VERSION_ONE,
			Extension:     headerExtension,
			ConnectionId:  b.connID,
			Timestamp:     int64(b.tsMicros),
			TimestampDiff: b.tsDiffMicros,
			WndSize:       b.windowSize,
			SeqNum:        b.seqNum,
			AckNum:        b.ackNum,
		},
		Eack: b.selectiveAck,
		Body: payload,
	}
}
