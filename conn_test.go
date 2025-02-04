package utp_go

import (
	"math"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"
)

const (
	TEST_BUFFER_SIZE     = 2048
	TEST_DELAYtime       = time.Millisecond * 100
	TEST_INITIAL_TIMEOUT = time.Second
)

func CreateTestConnection(endpoint Endpoint) *connection {
	// Create channels
	socketEvents := make(chan *socketEvent, 100)
	reads := make(chan *readOrWriteResult, 100)

	// Create peer address
	peer := &TcpPeer{&net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 3400,
	},
	}

	// Create connection ID
	cid := ConnectionId{
		Send: 101,
		Recv: 100,
		Peer: peer,
	}

	unackTimeoutCh := make(chan *packet, 1000)
	handleExpiration := func(key any, pkt *packet) {
		unackTimeoutCh <- pkt
	}
	conn := &connection{
		logger:         log.Root(),
		state:          NewConnState(make(chan error, 1)),
		cid:            &cid,
		config:         NewConnectionConfig(),
		endpoint:       &endpoint,
		peerTsDiff:     100 * time.Millisecond,
		peerRecvWindow: math.MaxUint32,
		socketEvents:   socketEvents,
		unacked:        newTimeWheel[*packet](TEST_INITIAL_TIMEOUT/4, 8, handleExpiration),
		reads:          reads,
		readable:       make(chan struct{}, 1),
		pendingWrites:  make([]*queuedWrite, 0),
		writable:       make(chan struct{}, 1),
		latestTimeout:  nil,
	}

	return conn
}

func TestOnSynInitiator(t *testing.T) {
	syn := uint16(100)
	endpoint := Endpoint{
		Type:   Initiator,
		SynNum: syn,
	}
	conn := CreateTestConnection(endpoint)

	conn.onSyn(syn)

	require.Equal(t, ConnClosed, conn.state.stateType, "expected state to be closed")
	require.ErrorIs(t, conn.state.Err, ErrSynFromAcceptor, "expected error to be %v, got %v", ErrSynFromAcceptor, conn.state.Err)
}

func TestOnSynAcceptor(t *testing.T) {
	// Setup
	syn := uint16(100)
	synAck := uint16(101)
	endpoint := Endpoint{
		Type:   Acceptor,
		SynNum: syn,
		SynAck: synAck,
	}

	conn := CreateTestConnection(endpoint)

	conn.onSyn(syn)
	require.Equal(t, ConnConnecting, conn.state.stateType, "expected state to be connecting, got %v", conn.state.stateType)
}

func TestOnSynAcceptorNonMatchingSyn(t *testing.T) {
	// Step 1: Setup the connection with expected syn
	syn := uint16(100)
	synAck := uint16(101)
	endpoint := Endpoint{
		Type:   Acceptor,
		SynNum: syn,
		SynAck: synAck,
	}
	conn := CreateTestConnection(endpoint)

	// Step 2: Try with different syn value
	altSyn := uint16(128)
	conn.onSyn(altSyn)
	require.Equal(t, ConnClosed, conn.state.stateType,
		"expected state to be closed, got %v", conn.state.stateType)
	require.ErrorIs(t, conn.state.Err, ErrInvalidSyn,
		"expected error to be %v, got %v", ErrInvalidSyn, conn.state.Err)
}

func TestOnStateConnectingInitiator(t *testing.T) {
	// Setup
	syn := uint16(100)
	endpoint := Endpoint{
		Type:   Initiator,
		SynNum: syn,
	}
	conn := CreateTestConnection(endpoint)

	// Test
	seqNum := uint16(1)
	conn.onState(seqNum, syn)

	// Verify
	require.Equal(t, ConnConnected, conn.state.stateType,
		"expected state to be connected, got %v", conn.state.stateType)
	require.Nil(t, conn.state.closing, "expected closing to be nil")
}

func TestOnPacketInvalidAckNum(t *testing.T) {
	// Setup connection
	syn := uint16(100)
	synAck := uint16(101)
	endpoint := Endpoint{
		Type:   Acceptor,
		SynNum: syn,
		SynAck: synAck,
	}
	conn := CreateTestConnection(endpoint)

	// Setup congestion control
	congestionCtrl := newDefaultController(fromConnConfig(conn.config))
	sentPackets := newSentPacketsWithoutLogger(synAck, congestionCtrl)

	// Send a packet
	data := []byte{0xef}
	length := uint32(64)
	now := time.Now()
	sentPackets.OnTransmit(
		synAck+1,
		st_data,
		data,
		length,
		now,
	)

	// Setup buffers and state
	sendBuf := newSendBuffer(TEST_BUFFER_SIZE)
	recvBuf := newReceiveBuffer(TEST_BUFFER_SIZE, syn)

	conn.state = &ConnState{
		stateType:   ConnConnected,
		SentPackets: sentPackets,
		SendBuf:     sendBuf,
		RecvBuf:     recvBuf,
	}

	// Process invalid ack
	delay := 100 * time.Millisecond
	ackResult := conn.processAck(synAck+2, nil, delay, now)

	// Verify results
	require.Equal(t, ConnClosed, conn.state.stateType, "expected state to be closed")
	require.ErrorIs(t, conn.state.Err, ErrInvalidAckNum, "expected error to be %v, got %v", ErrInvalidAckNum, conn.state.Err)

	require.Error(t, ackResult, "expected processAck to return an error")
}

func TestOnFinEstablished(t *testing.T) {
	// Setup
	syn := uint16(100)
	synAck := uint16(101)
	endpoint := Endpoint{
		Type:   Acceptor,
		SynNum: syn,
		SynAck: synAck,
	}
	conn := CreateTestConnection(endpoint)

	// Create buffers and controllers
	sendBuf := newSendBuffer(TEST_BUFFER_SIZE)
	congestionCtrl := newDefaultController(fromConnConfig(conn.config))
	sentPackets := newSentPacketsWithoutLogger(synAck, congestionCtrl)
	recvBuf := newReceiveBuffer(TEST_BUFFER_SIZE, syn)

	// Set connected state
	conn.state = &ConnState{
		stateType:   ConnConnected,
		SendBuf:     sendBuf,
		SentPackets: sentPackets,
		RecvBuf:     recvBuf,
	}

	// Test FIN
	fin := syn + 3 // wrapping add in Go handles overflow automatically
	err := conn.onFin(fin, []byte{})
	require.NoError(t, err, "expected no error")

	// Verify state
	require.Equal(t, ConnConnected, conn.state.stateType,
		"expected state to be Connected, got %v", conn.state.stateType)
	require.NotNil(t, conn.state.closing, "expected closing to be not nil")
	require.Nil(t, conn.state.closing.LocalFin, "expected closing.LocalFin to be nil")
	require.Equal(t, fin, *conn.state.closing.RemoteFin,
		"expected closing.RemoteFin to be %d, got %d", fin, *conn.state.closing.RemoteFin)
}

func TestOnFinClosing(t *testing.T) {
	// Step 1: Setup initial connection
	syn := uint16(100)
	synAck := uint16(101)
	endpoint := Endpoint{
		Type:   Acceptor,
		SynNum: syn,
		SynAck: synAck,
	}
	conn := CreateTestConnection(endpoint)

	// Step 2: Setup buffers and controllers
	sendBuf := newSendBuffer(TEST_BUFFER_SIZE)
	congestionCtrl := newDefaultController(fromConnConfig(conn.config))
	sentPackets := newSentPacketsWithoutLogger(synAck, congestionCtrl)
	recvBuf := newReceiveBuffer(TEST_BUFFER_SIZE, syn)

	// Step 3: Set local fin
	localFin := synAck + 3
	conn.state = &ConnState{
		stateType:   ConnConnected,
		SendBuf:     sendBuf,
		SentPackets: sentPackets,
		RecvBuf:     recvBuf,
		closing: &ClosingRecord{
			LocalFin:  &localFin,
			RemoteFin: nil,
		},
	}

	// Step 4: Test remote fin
	remoteFin := syn + 3
	err := conn.onFin(remoteFin, []byte{})
	require.NoError(t, err, "expected no error")

	// Step 5: Verify state
	require.Equal(t, ConnConnected, conn.state.stateType,
		"expected state to be connected, got %v", conn.state.stateType)
	require.NotNil(t, conn.state.closing, "expected closing to be not nil")
	require.NotNil(t, conn.state.closing.LocalFin, "expected closing.LocalFin to be not nil")
	require.Equal(t, localFin, *conn.state.closing.LocalFin,
		"expected closing.LocalFin to be %d, got %d", localFin, *conn.state.closing.LocalFin)
	require.NotNil(t, conn.state.closing.RemoteFin, "expected closing.RemoteFin to be not nil")
	require.Equal(t, remoteFin, *conn.state.closing.RemoteFin,
		"expected closing.LocalFin to be %d, got %d", remoteFin, *conn.state.closing.RemoteFin)
}

func TestOnFinClosingNonMatchingFin(t *testing.T) {
	// Step 1: Setup connection
	syn := uint16(100)
	synAck := uint16(101)
	endpoint := Endpoint{
		Type:   Acceptor,
		SynNum: syn,
		SynAck: synAck,
	}
	conn := CreateTestConnection(endpoint)

	// Step 2: Setup buffers
	sendBuf := newSendBuffer(TEST_BUFFER_SIZE)
	congestionCtrl := newDefaultController(fromConnConfig(conn.config))
	sentPackets := newSentPacketsWithoutLogger(synAck, congestionCtrl)
	recvBuf := newReceiveBuffer(TEST_BUFFER_SIZE, syn)

	// Step 3: Set initial fin
	fin := syn + 3
	conn.state = &ConnState{
		stateType:   ConnConnected,
		SendBuf:     sendBuf,
		SentPackets: sentPackets,
		RecvBuf:     recvBuf,
		closing: &ClosingRecord{
			LocalFin:  nil,
			RemoteFin: &fin,
		},
	}

	// Step 4: Test with different fin
	altFin := fin + 1
	err := conn.onFin(altFin, []byte{})
	require.NoError(t, err, "expected no error")

	// Step 5: Verify error state
	require.Equal(t, ConnClosed, conn.state.stateType, "expected state to be closed")
	require.ErrorIs(t, conn.state.Err, ErrInvalidFin,
		"expected error to be %v, got %v", ErrInvalidFin, conn.state.Err)
}

func TestOnResetNonClosed(t *testing.T) {
	// Setup
	syn := uint16(100)
	endpoint := Endpoint{
		Type:   Initiator,
		SynNum: syn,
	}
	conn := CreateTestConnection(endpoint)

	// Test reset
	conn.onReset()

	// Verify state
	require.Equal(t, ConnClosed, conn.state.stateType, "expected state to be closed")
	require.ErrorIs(t, conn.state.Err, ErrReset, "expected error %v, got %v", ErrReset, conn.state.Err)
}

func TestOnResetClosed(t *testing.T) {
	// Setup
	syn := uint16(100)
	endpoint := Endpoint{
		Type:   Initiator,
		SynNum: syn,
	}
	conn := CreateTestConnection(endpoint)

	// Set initial closed state
	conn.state = &ConnState{
		stateType: ConnClosed,
	}

	// Test reset
	conn.onReset()

	// Verify state remains unchanged
	require.Equal(t, ConnClosed, conn.state.stateType, "expected state to be closed")
	require.NoError(t, conn.state.Err, "expected error to be nil")
}
