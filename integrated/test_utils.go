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

type LinkDecider interface {
	shouldSend() bool
}

type ManualLinkDecider struct {
	upSwitch atomic.Bool
}

func newManualLinkDecider() *ManualLinkDecider {
	decider := &ManualLinkDecider{}
	decider.upSwitch.Store(true)
	return decider
}

func (d *ManualLinkDecider) shouldSend() bool {
	return d.upSwitch.Load()
}

type DropFirstNSent struct {
	targetDropsN int
	curDropsN    int
}

func newDropFirstNSent(dropsN int) *DropFirstNSent {
	return &DropFirstNSent{
		targetDropsN: dropsN,
		curDropsN:    0,
	}
}

func (d *DropFirstNSent) shouldSend() bool {
	if d.curDropsN < d.targetDropsN {
		d.curDropsN += 1
		return false
	} else {
		return true
	}
}

type MockConnectedPeer struct {
	name string
}

func (p *MockConnectedPeer) Hash() string {
	return p.name
}

type MockUdpSocket struct {
	sendCh      chan []byte
	recvCh      chan []byte
	onlyPeer    utp.ConnectionPeer
	linkDecider LinkDecider
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
	if !s.linkDecider.shouldSend() {
		log.Debug("Dropping packet", "dst.peer", dst.Hash(), "onlyPeer", s.onlyPeer.Hash())
		return len(b), nil
	}
	s.sendCh <- b
	log.Debug("Sent a packet out to dest", "dest.peer", dst, "len", len(b), "data", hex.EncodeToString(b))
	return len(b), nil
}
func (s *MockUdpSocket) Close() error {
	return nil
}

func buildLinkPair(aDecider LinkDecider, bDecider LinkDecider) (*MockUdpSocket, *MockUdpSocket) {
	peerA, peerB := &MockConnectedPeer{name: "peerA"}, &MockConnectedPeer{name: "peerB"}
	peerACh, peerBCh := make(chan []byte, 1), make(chan []byte, 1)

	// A -> B
	a, b := &MockUdpSocket{
		sendCh:      peerACh,
		recvCh:      peerBCh,
		onlyPeer:    peerB,
		linkDecider: aDecider,
	}, &MockUdpSocket{
		// B -> A
		sendCh:      peerBCh,
		recvCh:      peerACh,
		onlyPeer:    peerA,
		linkDecider: bDecider,
	}
	return a, b
}

func buildCidPair(socketA *MockUdpSocket, socketB *MockUdpSocket, lowerId uint16) (*utp.ConnectionId, *utp.ConnectionId) {
	higherId := lowerId + 1
	cidA, cidB := utp.NewConnectionId(socketA.onlyPeer, lowerId, higherId), utp.NewConnectionId(socketB.onlyPeer, higherId, lowerId)
	return cidA, cidB
}

func buildConnectedPair() (*MockUdpSocket, *utp.ConnectionId, *MockUdpSocket, *utp.ConnectionId) {
	socketA, socketB := buildLinkPair(newManualLinkDecider(), newManualLinkDecider())
	cidA, cidB := buildCidPair(socketA, socketB, 100)
	return socketA, cidA, socketB, cidB
}

func buildLinkDropSentPair(n int) (*MockUdpSocket, *utp.ConnectionId, *MockUdpSocket, *utp.ConnectionId) {
	socketA, socketB := buildLinkPair(newManualLinkDecider(), newDropFirstNSent(n))
	cidA, cidB := buildCidPair(socketA, socketB, 100)
	return socketA, cidA, socketB, cidB
}
