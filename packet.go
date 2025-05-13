package utp_go

import (
	"encoding/binary"
	"errors"
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
	st_data PacketType = iota
	st_fin
	st_state
	st_reset
	st_syn
)

var (
	ErrInvalidHeaderSize           = errors.New("invalid header size")
	ErrInvalidPacketVersion        = errors.New("invalid packet version")
	ErrInvalidPacketType           = errors.New("invalid packet type")
	ErrInvalidExtensionType        = errors.New("invalid extension type")
	ErrInsufficientLen             = errors.New("insufficient length for extension")
	ErrPacketTooShort              = errors.New("packet too short for selective ack extension")
	ErrInsufficientSelectiveAckLen = errors.New("insufficient length for selective ACK")
	ErrInvalidSelectiveAckLen      = errors.New("invalid length for selective ACK")
)

type PacketType byte

func (p PacketType) Check() error {
	if p > 4 {
		return ErrInvalidPacketType
	}
	return nil
}

func (p *PacketType) String() string {
	switch *p {
	case st_data:
		return "st_data"
	case st_fin:
		return "st_fin"
	case st_state:
		return "st_state"
	case st_reset:
		return "st_reset"
	case st_syn:
		return "st_syn"
	default:
		return "UNKNOWN"
	}
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
	typeVer := byte(h.PacketType)<<4 | h.Version
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
	packetTypeVal := PacketType(packetType)
	if err := packetTypeVal.Check(); err != nil {
		return nil, err
	}

	version := value[0] & 0x0F
	versionVal := version

	extension := value[1]

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
		return nil, ErrInsufficientSelectiveAckLen
	}
	if len(data)%4 != 0 {
		return nil, ErrInvalidSelectiveAckLen
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

type packet struct {
	Header *PacketHeaderV1
	Eack   *SelectiveAck
	Body   []byte
}

func (p *packet) EncodedLen() int {
	length := MINIMAL_HEADER_SIZE

	if p.Eack != nil {
		length += p.Eack.EncodedLen() + EXTENSION_TYPE_LEN + EXTENSION_LEN_LEN
	}

	length += len(p.Body)

	return length
}

func (p *packet) Encode() []byte {
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

func DecodePacket(b []byte) (*packet, error) {
	var p packet
	receivedBytesLength := len(b)
	if receivedBytesLength < MINIMAL_HEADER_SIZE {
		return nil, ErrInvalidHeaderSize
	}

	header, err := DecodePacketHeader(b[:MINIMAL_HEADER_SIZE])
	if err != nil {
		return nil, err
	}

	extensions, extensionsLen, err := DecodeRawExtensions(header.Extension, b[MINIMAL_HEADER_SIZE:])
	if err != nil {
		return nil, err
	}

	var extension ExtensionData
	for _, extensionData := range extensions {
		if extensionData.extension == 1 {
			extension = extensionData
		}
	}
	var ack *SelectiveAck
	if len(extension.payload) != 0 {
		if ack, err = DecodeSelectiveAck(extension.payload); err != nil {
			return nil, err
		}
	}

	payloadStartIndex := MINIMAL_HEADER_SIZE + extensionsLen
	var payload []byte
	if len(b) == payloadStartIndex {
		payload = make([]byte, 0)
	} else {
		payload = b[payloadStartIndex:]
	}
	if header.PacketType == st_data && len(payload) == 0 {
		return nil, ErrEmptyDataPayload
	}
	p.Header = header
	p.Eack = ack
	p.Body = payload
	return &p, nil
}

type ExtensionData struct {
	extension byte
	payload   []byte
}

func DecodeRawExtensions(firstExt byte, data []byte) ([]ExtensionData, int, error) {
	ext := firstExt
	index := 0
	extensions := make([]ExtensionData, 0)

	for ext != 0 {
		// Check if enough data for header (2 bytes)
		if len(data[index:]) < 2 {
			return nil, 0, ErrInsufficientLen
		}

		// Read next extension type
		nextExt := data[index]

		// Read extension length
		extLen := int(data[index+1])

		// Calculate start of extension data
		extStart := index + 2

		// Check if enough data for extension content
		if len(data[extStart:]) < extLen {
			return nil, 0, ErrInsufficientLen
		}

		// Copy extension data
		extData := make([]byte, extLen)
		copy(extData, data[extStart:extStart+extLen])

		// Add to extensions list
		extensions = append(extensions, ExtensionData{
			extension: ext,
			payload:   extData,
		})

		// Move to next extension
		ext = nextExt
		index = extStart + extLen
	}

	return extensions, index, nil
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

func (b *PacketBuilder) Build() *packet {
	var headerExtension byte
	if b.selectiveAck != nil {
		headerExtension = 1
	}

	var payload []byte
	if b.payload != nil {
		payload = b.payload
	}

	return &packet{
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
