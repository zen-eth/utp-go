package integrated

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
	utp "github.com/zen-eth/utp-go"
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
	upStatus *atomic.Bool
}

func (s *MockUdpSocket) ReadFrom(b []byte) (int, utp.ConnectionPeer, error) {
	buf, ok := <-s.recvCh
	if !ok {
		return 0, nil, ErrChClosed
	}
	n := len(buf)
	if len(b) < len(buf) {
		return 0, nil, ErrChClosed
	}
	log.Debug("read a raw packet from mocksocket", "from", s.onlyPeer, "len(buf)", len(buf), "buf", hex.EncodeToString(buf))
	copy(b[:n], buf)
	return n, s.onlyPeer, nil
}
func (s *MockUdpSocket) WriteTo(b []byte, dst utp.ConnectionPeer) (int, error) {
	if dst.Hash() != s.onlyPeer.Hash() {
		panic(fmt.Sprintf("MockUdpSocket only supports Writing To one peer: dst.peer = %s, onlyPeer = %s", dst.Hash(), s.onlyPeer.Hash()))
	}
	if !s.upStatus.Load() {
		log.Debug("MockUdpSocket is down, cannot send packet")
		return len(b), nil
	}
	s.sendCh <- b
	log.Debug("Sent a packet out to dest", "dest.peer", dst, "len", len(b), "data", hex.EncodeToString(b))
	return len(b), nil
}
func (s *MockUdpSocket) Close() error {
	return nil
}

func buildLinkPair() (*MockUdpSocket, *MockUdpSocket) {
	peerA, peerB := &MockConnectedPeer{name: "peerA"}, &MockConnectedPeer{name: "peerB"}
	peerACh, peerBCh := make(chan []byte, 1), make(chan []byte, 1)

	// A -> B
	a, b := &MockUdpSocket{
		sendCh:   peerACh,
		recvCh:   peerBCh,
		onlyPeer: peerB,
		upStatus: &atomic.Bool{},
	}, &MockUdpSocket{
		// B -> A
		sendCh:   peerBCh,
		recvCh:   peerACh,
		onlyPeer: peerA,
		upStatus: &atomic.Bool{},
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
