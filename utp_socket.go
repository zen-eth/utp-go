package utp_go

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const (
	MAX_UDP_PAYLOAD_SIZE         = math.MaxUint16
	CidGenerationTryWarningCount = 10
	AWAITING_CONNECTION_TIMEOUT  = time.Second * 20
)

var (
	ErrConnect = errors.New("utp_socket: connect error")
)

type PeerInfo interface {
	Hash() [32]byte
}

type Conn interface {
	ReadFrom(b []byte) (int, ConnectionPeer, error)
	WriteTo(b []byte, dst ConnectionPeer) (int, error)
	Close() error
}

type StreamResult struct {
	stream *UtpStream
	err    error
}

type Accept struct {
	stream        chan *StreamResult
	config        *ConnectionConfig
	cid           *ConnectionId
	hasCid        bool
	awaitingTimer *time.Timer
}

type UdpConn struct {
	base *net.UDPConn
}

func (c *UdpConn) ReadFrom(b []byte) (int, ConnectionPeer, error) {
	n, addr, err := c.base.ReadFrom(b)
	if err != nil {
		return 0, nil, err
	}
	udpAddr := addr.(*net.UDPAddr)
	return n, &UdpPeer{addr: udpAddr}, nil
}
func (c *UdpConn) WriteTo(b []byte, dst ConnectionPeer) (int, error) {
	if dst == nil {
		return 0, fmt.Errorf("utp_socket: can't write to nil connection")
	}
	switch baseDst := dst.(type) {
	case *ConnectionId:
		return c.WriteTo(b, baseDst.Peer)
	case *UdpPeer:
		return c.base.WriteToUDP(b, baseDst.addr)
	}
	return 0, nil
}

func (c *UdpConn) Close() error {
	return c.base.Close()
}

type IncomingPacketRaw struct {
	peer    ConnectionPeer
	payload []byte
}

type IncomingPacket struct {
	pkt *Packet
	cid *ConnectionId
}

type UtpSocket struct {
	ctx             context.Context
	cancel          context.CancelFunc
	logger          log.Logger
	connsMutex      sync.Mutex
	conns           map[string]chan StreamEvent
	accepts         chan *Accept
	acceptsWithCid  chan *Accept
	socketEvents    chan SocketEvent
	awaitingMu      sync.Mutex
	awaiting        map[string]*Accept
	incomingConnsMu sync.Mutex
	incomingConns   map[string]*IncomingPacket
	socket          Conn
	readNextCh      chan struct{}
	incomingBuf     chan *IncomingPacketRaw
}

func Bind(network string, addr *net.UDPAddr) (Conn, error) {
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		return nil, err
	}
	return &UdpConn{conn}, nil
}

func NewWithSocket(ctx context.Context, socket Conn, logger log.Logger) *UtpSocket {
	ctx, cancel := context.WithCancel(ctx)

	utp := &UtpSocket{
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger.With("utp", "utp"),
		conns:          make(map[string]chan StreamEvent),
		accepts:        make(chan *Accept),
		acceptsWithCid: make(chan *Accept),
		socketEvents:   make(chan SocketEvent),
		awaiting:       make(map[string]*Accept),
		incomingConns:  make(map[string]*IncomingPacket),
		socket:         socket,
		readNextCh:     make(chan struct{}, 10),
		incomingBuf:    make(chan *IncomingPacketRaw, 10),
	}

	go utp.readLoop()
	go utp.eventLoop()

	return utp
}

func (s *UtpSocket) readLoop() {
	buf := make([]byte, math.MaxUint16)
	for range s.readNextCh {
		n, from, err := s.socket.ReadFrom(buf)
		if netutil.IsTemporaryError(err) {
			// Ignore temporary read errors.
			s.logger.Debug("Temporary UDP read error", "err", err)
			continue
		} else if err != nil {
			// Shut down the loop for permanent errors.
			if !errors.Is(err, io.EOF) {
				s.logger.Debug("UDP read error", "err", err)
			}
			return
		}
		dstBuf := make([]byte, n)
		copy(dstBuf, buf[:n])
		s.incomingBuf <- &IncomingPacketRaw{peer: from, payload: dstBuf}
	}
}

func (s *UtpSocket) eventLoop() {
	for {
		select {
		case event := <-s.socketEvents:
			switch event.Type {
			case Outgoing:
				encoded := event.Packet.Encode()
				if _, err := s.socket.WriteTo(encoded, event.ConnectionId); err != nil {
					//logger.Debug("Failed to send uTP packet",
					//	"error", err,
					//	"cid", e.Packet.ConnID,
					//	"type", e.Packet.Type)
				}

			case SocketShutdown:
				//logger.Debug("uTP conn shutdown",
				//	"send_id", e.CID.Send,
				//	"recv_id", e.CID.Recv)
				delete(s.conns, event.ConnectionId.Hash())
			}
		case acceptWithCid := <-s.acceptsWithCid:
			incomingConnsKey := acceptWithCid.cid.Hash()

			if incomingConn, exists := s.incomingConns[incomingConnsKey]; exists {
				delete(s.incomingConns, incomingConnsKey)
				// todo handle expire
				//delete(s.incomingConnsExpirations, incomingConnsKey)
				synPkt := incomingConn.pkt
				s.selectAcceptHelper(s.ctx, acceptWithCid.cid, synPkt, s.conns, acceptWithCid, s.socketEvents)
			} else {
				s.awaiting[incomingConnsKey] = acceptWithCid
				// todo awaitingTimer
				//awaitingExpirations[cidHash] = time.Now()
				continue
			}

		case accept := <-s.accepts:
			for cidHash, incomingAccept := range s.incomingConns {
				delete(s.incomingConns, cidHash)
				// todo handle expire
				//delete(s.incomingConnsExpirations, cidHash)
				s.selectAcceptHelper(s.ctx, incomingAccept.cid, incomingAccept.pkt, s.conns, accept, s.socketEvents)
				break
			}
		case incomingRaw := <-s.incomingBuf:
			// Handle incoming packets

			var packetPtr *Packet
			err := packetPtr.Decode(incomingRaw.payload)
			if err != nil {
				s.logger.Warn("Unable to decode uTP packet", "peer", incomingRaw.peer)
				continue
			}

			peerInitCID := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeSendIdPeerInitiated)
			weInitCID := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeSendIdWeInitiated)
			accCID := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeRecvId)

			// Look for existing connection
			if conn := s.getConn(accCID, weInitCID, peerInitCID); conn != nil {
				conn <- StreamEvent{
					Type:   StreamIncoming,
					Packet: packetPtr,
				}
				continue
			} else {
				if packetPtr.Header.PacketType == ST_SYN {
					// peer
					cid := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeRecvId)
					cidHash := cid.Hash()
					if accept, exist := s.awaiting[cidHash]; exist {
						connected := make(chan error, 1)
						stream := NewUtpStream(s.ctx, cid, accept.config, packetPtr, s.socketEvents, make(chan StreamEvent, 10), connected)
						go s.awaitConnected(stream, accept, connected)
					} else {
						s.incomingConns[cidHash] = &IncomingPacket{pkt: packetPtr, cid: cid}
						// todo incomingConnsExpirations insert
					}
				} else {
					s.logger.Debug("received uTP packet for non-existing conn",
						"cid", packetPtr.Header.ConnectionId,
						"packetType", packetPtr.Header.PacketType,
						"seq", packetPtr.Header.SeqNum,
						"ack", packetPtr.Header.AckNum,
						"peerInitCID", peerInitCID,
						"weInitCid", weInitCID,
						"accCID", accCID)
					if packetPtr.Header.PacketType != ST_RESET {
						randSeqNum := RandomUint16()
						resetPacket := NewPacketBuilder(ST_RESET, packetPtr.Header.ConnectionId, uint32(time.Now().UnixMicro()), 100_000, randSeqNum).Build()
						s.socketEvents <- SocketEvent{Outgoing, resetPacket, incomingRaw.peer}
					}
				}
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *UtpSocket) NumConnections() int {
	return len(s.conns)
}

func (s *UtpSocket) Close() {
	s.cancel()
	close(s.readNextCh)
}

func (s *UtpSocket) Cid(peer ConnectionPeer, isInitiator bool) *ConnectionId {
	return s.GenerateCid(peer, isInitiator, nil)
}

func (s *UtpSocket) GenerateCid(peer ConnectionPeer, isInitiator bool, eventCh chan StreamEvent) *ConnectionId {
	cid := &ConnectionId{
		Peer: peer,
	}

	generationAttemptCount := 0

	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	for {
		if generationAttemptCount > CidGenerationTryWarningCount {
			s.logger.Warn("tried to generate a cid %d times", generationAttemptCount)
		}

		// Generate random recv ID
		recv := RandomUint16()
		var send uint16
		// Calculate send ID based on initiator status
		if isInitiator {
			send = recv + 1
		} else {
			send = recv - 1
		}

		cid.Send = send
		cid.Recv = recv

		_, exists := s.conns[cid.Hash()]

		if !exists {
			if eventCh != nil {
				s.conns[cid.Hash()] = eventCh
			}
			return cid
		}

		generationAttemptCount++
	}
}

func (s *UtpSocket) Accept(ctx context.Context, config *ConnectionConfig) (*UtpStream, error) {
	streamCh := make(chan *StreamResult, 1)
	accept := &Accept{
		stream: streamCh,
		config: config,
		hasCid: false,
	}

	// Send accept request through channel
	select {
	case s.accepts <- accept:
	}

	// Wait for stream or timeout
	select {
	case streamRes := <-streamCh:
		if streamRes == nil {
			return nil, fmt.Errorf("stream creation failed")
		}
		return streamRes.stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *UtpSocket) AcceptWithCid(ctx context.Context, cid *ConnectionId, config *ConnectionConfig) (*UtpStream, error) {
	streamCh := make(chan *StreamResult, 1)
	accept := &Accept{
		stream: streamCh,
		config: config,
		hasCid: true,
		cid:    cid,
	}

	// Send accept request through channel
	s.acceptsWithCid <- accept

	// Wait for stream or timeout
	select {
	case streamRes := <-streamCh:
		if streamRes == nil {
			return nil, fmt.Errorf("stream creation failed")
		}
		return streamRes.stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *UtpSocket) Connect(ctx context.Context, peer ConnectionPeer, config *ConnectionConfig) (*UtpStream, error) {
	// Create channels for connection status and events
	connectedCh := make(chan error, 1)
	eventsCh := make(chan StreamEvent)

	// Generate connection ID
	cid := s.GenerateCid(peer, true, eventsCh)

	// Create new UTP stream
	stream := NewUtpStream(
		ctx,
		cid,
		config,
		nil,
		s.socketEvents,
		eventsCh,
		connectedCh,
	)

	// Wait for connection result
	select {
	case err, ok := <-connectedCh:
		if ok && err == nil {
			return stream, nil
		} else if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("connection timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *UtpSocket) ConnectWithCid(
	ctx context.Context,
	cid *ConnectionId,
	config *ConnectionConfig,
) (*UtpStream, error) {
	_, exists := s.conns[cid.Hash()]
	if exists {
		return nil, fmt.Errorf("connection ID unavailable")
	}

	connected := make(chan error, 1)
	streamEvent := make(chan StreamEvent, 100)

	s.conns[cid.Hash()] = streamEvent

	stream := NewUtpStream(
		ctx,
		cid,
		config,
		nil,
		s.socketEvents,
		streamEvent,
		connected,
	)

	_, ok := <-connected
	if ok {
		return stream, nil
	} else {
		//log.Error().
		//	Interface("cid", cid).
		//	Msg("failed to open connection")
		return nil, fmt.Errorf("connection timed out")
	}

	//log.Error().
	//	Err(err).
	//	Interface("cid", cid).
	//	Msg("failed to open connection")
	//return nil, err
}

func (s *UtpSocket) awaitConnected(
	stream *UtpStream,
	accept *Accept,
	connected <-chan error,
) {
	err, ok := <-connected
	if err == nil && ok {
		accept.stream <- &StreamResult{stream: stream}
		return
	} else if err != nil {
		accept.stream <- &StreamResult{err: fmt.Errorf("connection failed")}
		return
	}

	accept.stream <- &StreamResult{err: fmt.Errorf("connection aborted")}
	return
}

func (s *UtpSocket) selectAcceptHelper(
	ctx context.Context,
	cid *ConnectionId,
	syn *Packet,
	conns map[string]chan StreamEvent,
	accept *Accept,
	socketEventTx chan<- SocketEvent,
) {
	_, exists := conns[cid.Hash()]
	if exists {
		accept.stream <- &StreamResult{
			stream: nil,
			err:    fmt.Errorf("connection ID unavailable"),
		}
		return
	}

	connected := make(chan error, 1)
	streamEvent := make(chan StreamEvent, 100)

	conns[cid.Hash()] = streamEvent

	stream := NewUtpStream(
		ctx,
		cid,
		accept.config,
		syn,
		socketEventTx,
		streamEvent,
		connected,
	)

	go s.awaitConnected(stream, accept, connected)
}

func (s *UtpSocket) getConn(peerInitCid *ConnectionId, ourInitCid *ConnectionId, accCid *ConnectionId) chan<- StreamEvent {
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	if ch, exist := s.conns[peerInitCid.Hash()]; exist {
		return ch
	}
	if ch, exist := s.conns[ourInitCid.Hash()]; exist {
		return ch
	}
	if ch, exist := s.conns[accCid.Hash()]; exist {
		return ch
	}
	return nil
}

type IdType int

const (
	IdTypeRecvId IdType = iota
	IdTypeSendIdWeInitiated
	IdTypeSendIdPeerInitiated
)

func CidFromPacket(
	packet *Packet,
	src ConnectionPeer,
	idType IdType,
) *ConnectionId {
	var send, recv uint16

	switch idType {
	case IdTypeRecvId:
		switch packet.Header.PacketType {
		case ST_SYN:
			send = packet.Header.ConnectionId
			recv = packet.Header.ConnectionId + 1 // wrapping add
		case ST_STATE, ST_DATA, ST_FIN, ST_RESET: // State, Data, Fin, Reset
			send = packet.Header.ConnectionId - 1 // wrapping sub
			recv = packet.Header.ConnectionId
		}

	case IdTypeSendIdWeInitiated:
		send = packet.Header.ConnectionId + 1 // wrapping add
		recv = packet.Header.ConnectionId

	case IdTypeSendIdPeerInitiated:
		send = packet.Header.ConnectionId
		recv = packet.Header.ConnectionId - 1 // wrapping sub
	}

	return &ConnectionId{
		Send: send,
		Recv: recv,
		Peer: src,
	}
}
