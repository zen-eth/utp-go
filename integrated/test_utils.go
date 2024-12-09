package integrated

import (
	"encoding/hex"
	"errors"
	"os"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
	utp "github.com/optimism-java/utp-go"
)

var (
	ErrChClosed      = errors.New("channel closed")
	ErrBufferToSmall = errors.New("buffer too small for perfect link")
)

type MockConnectedPeer struct {
	name string
}

func (p *MockConnectedPeer) Hash() string {
	return p.name
}

type MockUdpSocket struct {
	sendCh   chan []byte
	recvCh   chan []byte
	onlyPeer utp.ConnectionPeer
	upStatus atomic.Bool
}

func (s *MockUdpSocket) ReadFrom(b []byte) (int, utp.ConnectionPeer, error) {
	select {
	case buf, ok := <-s.recvCh:
		if !ok {
			return 0, nil, ErrChClosed
		}
		n := len(buf)
		if len(b) < len(buf) {
			return 0, nil, ErrChClosed
		}
		log.Debug("read a raw packet from mocksocket", "buf", hex.EncodeToString(buf))
		copy(b[:n], buf)
		return n, s.onlyPeer, nil
	}
}
func (s *MockUdpSocket) WriteTo(b []byte, dst utp.ConnectionPeer) (int, error) {
	if dst.Hash() == s.onlyPeer.Hash() {
		panic("MockUdpSocket only supports Writing To one peer")
	}
	if !s.upStatus.Load() {
		return len(b), nil
	}
	select {
	case s.sendCh <- b:
		log.Debug("Sent a packet out to dest", "dest.peer", dst, "len", len(b), "data", hex.EncodeToString(b))
		return len(b), nil
	}
	return 0, nil
}
func (s *MockUdpSocket) Close() error {
	return nil
}

func buildLinkPair() (*MockUdpSocket, *MockUdpSocket) {
	handler := log.NewTerminalHandler(os.Stdout, true)
	log.SetDefault(log.NewLogger(handler))
	peerA, peerB := &MockConnectedPeer{name: "peerA"}, &MockConnectedPeer{name: "peerB"}
	peerACh, peerBCh := make(chan []byte, 1), make(chan []byte, 1)

	a, b := &MockUdpSocket{
		sendCh:   peerACh,
		recvCh:   peerBCh,
		onlyPeer: peerB,
	}, &MockUdpSocket{
		sendCh:   peerBCh,
		recvCh:   peerACh,
		onlyPeer: peerA,
	}
	a.upStatus.Store(true)
	b.upStatus.Store(true)
	return a, b
}

func buildCidPair(socketA *MockUdpSocket, socketB *MockUdpSocket, lowerId uint16) (*utp.ConnectionId, *utp.ConnectionId) {
	higherId := lowerId + 1
	cidA, cidB := &utp.ConnectionId{
		Send: higherId,
		Recv: lowerId,
		Peer: socketA.onlyPeer,
	}, &utp.ConnectionId{
		Send: lowerId,
		Recv: higherId,
		Peer: socketB.onlyPeer,
	}
	return cidA, cidB
}

func buildConnectedPair() (*MockUdpSocket, *utp.ConnectionId, *MockUdpSocket, *utp.ConnectionId) {
	socketA, socketB := buildLinkPair()
	cidA, cidB := buildCidPair(socketA, socketB, 100)
	return socketA, cidA, socketB, cidB
}
