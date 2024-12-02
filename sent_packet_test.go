package utp_go

import (
	"testing"
	"time"

	"github.com/optimism-java/utp-go/rs"
	"go.uber.org/mock/gomock"
)

const TEST_DELAY = time.Duration(time.Millisecond * 100)

func TestNextSeqNum(t *testing.T) {

}

func TestOnTransmitInitial(t *testing.T) {
	mockContrcller := NewMockController(gomock.NewController(t))
	mockContrcller.EXPECT().BytesAvailableInWindow().Return(uint32(2048))
	// seqNum uint16, transmit Transmit, dataLen uint32
	mockContrcller.EXPECT().OnTransmit(uint16(0), Transmit(0), uint32(1)).Return(nil)
	initSeqNum := uint16(65535) // u16::MAX
	sentPackets := NewSentPackets(initSeqNum, mockContrcller)

	seqNum := sentPackets.NextSeqNum()
	data := []byte{0}
	length := uint32(len(data))
	now := time.Now()
	sentPackets.OnTransmit(seqNum, rs.Data, data, length, now)

	if len(sentPackets.packets) != 1 {
		t.Errorf("expected 1 packet, got %d", len(sentPackets.packets))
	}

	packetInst := sentPackets.packets[0]
	if packetInst.seqNum != seqNum {
		t.Errorf("expected seq num %d, got %d", seqNum, packetInst.seqNum)
	}
	if !packetInst.transmission.Equal(now) {
		t.Errorf("expected transmission time %v, got %v", now, packetInst.transmission)
	}
	if len(packetInst.acks) != 0 {
		t.Errorf("expected empty acks, got %d", len(packetInst.acks))
	}
	if len(packetInst.retransmissions) != 0 {
		t.Errorf("expected empty retransmissions, got %d", len(packetInst.retransmissions))
	}
}
