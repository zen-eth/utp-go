package utp_go

import (
	"context"
	"encoding/hex"
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
	ctx              context.Context
	cancel           context.CancelFunc
	logger           log.Logger
	connsMutex       sync.Mutex
	conns            map[string]chan *StreamEvent
	accepts          chan *Accept
	acceptsWithCidCh chan *Accept
	socketEvents     chan *SocketEvent
	awaitingMu       sync.Mutex
	awaiting         map[string]*Accept
	incomingConnsMu  sync.Mutex
	incomingConns    map[string]*IncomingPacket
	socket           Conn
	readNextCh       chan struct{}
	incomingBuf      chan *IncomingPacketRaw
}

func Bind(network string, addr *net.UDPAddr) (Conn, error) {
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		return nil, err
	}
	return &UdpConn{conn}, nil
}

func WithSocket(ctx context.Context, socket Conn, logger log.Logger) *UtpSocket {
	ctx, cancel := context.WithCancel(ctx)
	if logger == nil {
		logger = log.New("utp", "socket")
	}

	utp := &UtpSocket{
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger,
		conns:            make(map[string]chan *StreamEvent),
		accepts:          make(chan *Accept, 1),
		acceptsWithCidCh: make(chan *Accept, 1),
		socketEvents:     make(chan *SocketEvent, 10),
		awaiting:         make(map[string]*Accept),
		incomingConns:    make(map[string]*IncomingPacket),
		socket:           socket,
		readNextCh:       make(chan struct{}, 10),
		incomingBuf:      make(chan *IncomingPacketRaw, 10),
	}

	go utp.readLoop()
	go utp.eventLoop()

	return utp
}

func (s *UtpSocket) readLoop() {
	buf := make([]byte, math.MaxUint16)
	s.logger.Debug("utp socket readLoop start...")
	for range s.readNextCh {
		n, from, err := s.socket.ReadFrom(buf)
		s.logger.Debug("read data from base socket", "n", n, "from", from)
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
		s.logger.Debug("recv a packet from remote", "buf.len", n, "from", from, "buf", hex.EncodeToString(buf[:n]))
		s.incomingBuf <- &IncomingPacketRaw{peer: from, payload: dstBuf}
	}
}

func (s *UtpSocket) eventLoop() {
	s.readNextCh <- struct{}{}
	s.logger.Debug("utp socket eventLoop start...")
	for {
		select {
		case event := <-s.socketEvents:
			s.logger.Debug("a socket event should be sent to target", "event.type", event.Type, "event.cid", event.ConnectionId)
			switch event.Type {
			case Outgoing:
				s.logger.Debug("Send a packet out",
					"target.cid", event.ConnectionId,
					"packet.type", event.Packet.Header.PacketType,
					"packet.seqNum", event.Packet.Header.SeqNum,
					"packet.ackNum", event.Packet.Header.AckNum,
					"packet.body.len", len(event.Packet.Body))
				encoded := event.Packet.Encode()
				var peer ConnectionPeer
				if cid, ok := event.ConnectionId.(*ConnectionId); ok {
					peer = cid.Peer
				} else {
					peer = event.ConnectionId
				}
				if _, err := s.socket.WriteTo(encoded, peer); err != nil {
					s.logger.Debug("Failed to send uTP packet",
						"error", err,
						"cid", event.Packet.Header.ConnectionId,
						"type", event.Packet.Header.PacketType)
				}
				s.readNextCh <- struct{}{}

			case SocketShutdown:
				s.logger.Debug("uTP conn shutdown", "cid.Hash", event.ConnectionId.Hash())
				s.removeConnStream(event.ConnectionId.Hash())

			}
		case acceptWithCid := <-s.acceptsWithCidCh:
			s.handleNewAcceptWithCidEvent(acceptWithCid)
		case accept := <-s.accepts:
			s.handleNewAcceptEvent(accept)
		case incomingRaw := <-s.incomingBuf:
			// Handle incoming packets
			s.logger.Debug("receive incoming packet", "peer", incomingRaw.peer)
			packetPtr, err := DecodePacket(incomingRaw.payload)
			if err != nil {
				s.logger.Warn("Unable to decode uTP packet", "peer", incomingRaw.peer)
				continue
			}

			peerInitCID := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeSendIdPeerInitiated)
			weInitCID := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeSendIdWeInitiated)
			accCID := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeRecvId)
			// Look for existing connection
			if connStream := s.getConnStreamWithCids(accCID, weInitCID, peerInitCID); connStream != nil {
				s.logger.Debug("receive a packet from a exist conn stream",
					"src.peer", incomingRaw.peer,
					"packet.type", packetPtr.Header.PacketType,
					"packet.Data.len", len(packetPtr.Body))
				connStream <- &StreamEvent{
					Type:   StreamIncoming,
					Packet: packetPtr,
				}
			} else {
				if packetPtr.Header.PacketType == ST_SYN {
					cid := CidFromPacket(packetPtr, incomingRaw.peer, IdTypeRecvId)
					s.logger.Debug("receive a syn packet from a new conn stream",
						"src.peer", incomingRaw.peer,
						"cid.Send", cid.Send,
						"cid.Recv", cid.Recv,
						"cid.hash", cid.Hash())
					cidHash := cid.Hash()
					if accept, exist := s.getAwaiting(cidHash); exist {
						s.logger.Debug("found a accept request from awaiting map...",
							"accept.cid.Send", accept.cid.Send, "accept.cid.Recv", accept.cid.Recv)
						connected := make(chan error, 1)
						newConnStream := make(chan *StreamEvent, 10)
						s.putConnStream(cidHash, newConnStream)
						stream := NewUtpStream(s.ctx, s.logger, cid, accept.config, packetPtr, s.socketEvents, newConnStream, connected)
						go s.awaitConnected(stream, accept, connected)
					} else {
						s.logger.Debug("put a new syn packet to incomingConns...")
						s.putIncomingConn(cidHash, &IncomingPacket{pkt: packetPtr, cid: cid})
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
						s.socketEvents <- &SocketEvent{Outgoing, resetPacket, incomingRaw.peer}
					}
				}
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *UtpSocket) handleNewAcceptWithCidEvent(acceptWithCid *Accept) {
	s.logger.Debug("get a accept event with cid",
		"accept.cid.Send", acceptWithCid.cid.Send,
		"accept.cid.Recv",
		acceptWithCid.cid.Recv, "accept.cid.Peer", acceptWithCid.cid.Peer,
		"hash", acceptWithCid.cid.Hash())
	incomingConnsKey := acceptWithCid.cid.Hash()

	if incomingConn, exists := s.removeIncomingConn(incomingConnsKey); exists {
		s.logger.Debug("conn has already accepted", "key", incomingConnsKey)
		s.selectAcceptHelper(s.ctx, acceptWithCid.cid, incomingConn.pkt, acceptWithCid, s.socketEvents)
	} else {
		s.logger.Debug("will wait for the syn pkt arrive", "key", incomingConnsKey)
		s.putAwaiting(incomingConnsKey, acceptWithCid)
	}
}

func (s *UtpSocket) handleNewAcceptEvent(accept *Accept) {
	incomingAccept := s.nextIncomingConn()
	if incomingAccept != nil {
		s.selectAcceptHelper(s.ctx, incomingAccept.cid, incomingAccept.pkt, accept, s.socketEvents)
	} else {
		accept.stream <- &StreamResult{
			err: errors.New("no incoming conn"),
		}
	}
}

func (s *UtpSocket) getAwaiting(key string) (*Accept, bool) {
	s.awaitingMu.Lock()
	defer s.awaitingMu.Unlock()
	accept, ok := s.awaiting[key]
	return accept, ok
}

func (s *UtpSocket) putAwaiting(key string, acceptWithCid *Accept) {
	s.awaitingMu.Lock()
	defer s.awaitingMu.Unlock()
	s.awaiting[key] = acceptWithCid
	// todo awaitingTimer
	//awaitingExpirations[cidHash] = time.Now()
}

func (s *UtpSocket) putIncomingConn(key string, incomingConn *IncomingPacket) {
	s.incomingConnsMu.Lock()
	defer s.incomingConnsMu.Unlock()
	s.incomingConns[key] = incomingConn
	// todo incomingConnsExpirations insert
}

func (s *UtpSocket) removeIncomingConn(key string) (*IncomingPacket, bool) {
	s.incomingConnsMu.Lock()
	defer s.incomingConnsMu.Unlock()
	incomingConn, exists := s.incomingConns[key]
	if exists {
		// todo handle expire
		//delete(s.incomingConnsExpirations, incomingConnsKey)
		delete(s.incomingConns, key)
	}
	return incomingConn, exists
}

func (s *UtpSocket) nextIncomingConn() *IncomingPacket {
	s.incomingConnsMu.Lock()
	defer s.incomingConnsMu.Unlock()
	var cidHash string
	var incomingAccept *IncomingPacket
	for cidHash, incomingAccept = range s.incomingConns {
		break
	}
	if incomingAccept != nil {
		// todo handle expire
		//delete(s.incomingConnsExpirations, cidHash)
		delete(s.incomingConns, cidHash)
	}
	return incomingAccept
}

func (s *UtpSocket) NumConnections() int {
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	return len(s.conns)
}

func (s *UtpSocket) Close() {
	s.cancel()
	close(s.readNextCh)
}

func (s *UtpSocket) Cid(peer ConnectionPeer, isInitiator bool) *ConnectionId {
	return s.GenerateCid(peer, isInitiator, nil)
}

func (s *UtpSocket) GenerateCid(peer ConnectionPeer, isInitiator bool, eventCh chan *StreamEvent) *ConnectionId {
	cid := &ConnectionId{
		Peer: peer,
	}

	generationAttemptCount := 0
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

		if _, exists := s.getConnStream(cid.Hash()); !exists {
			if eventCh != nil {
				s.putConnStream(cid.Hash(), eventCh)
			}
			return cid
		}
		generationAttemptCount++
	}
}

func (s *UtpSocket) Accept(ctx context.Context, config *ConnectionConfig) (*UtpStream, error) {
	accept := &Accept{
		stream: make(chan *StreamResult, 1),
		config: config,
		hasCid: false,
	}

	// Send accept request through channel
	select {
	case s.accepts <- accept:
	}

	// Wait for stream or timeout
	select {
	case streamRes := <-accept.stream:
		if streamRes == nil {
			return nil, fmt.Errorf("stream creation failed")
		}
		return streamRes.stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *UtpSocket) AcceptWithCid(ctx context.Context, cid *ConnectionId, config *ConnectionConfig) (*UtpStream, error) {
	accept := &Accept{
		stream: make(chan *StreamResult, 1),
		config: config,
		hasCid: true,
		cid:    cid,
	}

	// Send accept request through channel
	s.acceptsWithCidCh <- accept

	// Wait for stream or timeout
	select {
	case streamRes := <-accept.stream:
		if streamRes == nil {
			return nil, fmt.Errorf("stream creation failed")
		}
		s.logger.Debug("accept success", "cid.Peer", streamRes.stream.cid.Peer, "cid.Send", streamRes.stream.cid.Send, "cid.Recv", streamRes.stream.cid.Recv)
		return streamRes.stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *UtpSocket) Connect(ctx context.Context, peer ConnectionPeer, config *ConnectionConfig) (*UtpStream, error) {
	// Create channels for connection status and events
	connectedCh := make(chan error, 1)
	streamEvents := make(chan *StreamEvent, 10)

	// Generate connection ID
	cid := s.GenerateCid(peer, true, streamEvents)

	// Create new UTP stream
	s.putConnStream(cid.Hash(), streamEvents)
	stream := NewUtpStream(
		ctx,
		s.logger,
		cid,
		config,
		nil,
		s.socketEvents,
		streamEvents,
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
	_, exists := s.getConnStream(cid.Hash())
	if exists {
		return nil, fmt.Errorf("connection ID unavailable")
	}

	connected := make(chan error, 1)
	streamEvents := make(chan *StreamEvent, 10)

	s.putConnStream(cid.Hash(), streamEvents)
	stream := NewUtpStream(
		ctx,
		s.logger,
		cid,
		config,
		nil,
		s.socketEvents,
		streamEvents,
		connected,
	)
	select {
	case _, ok := <-connected:
		if ok {
			return stream, nil
		} else {
			s.logger.Error("failed to open connection", "cid.send", cid.Send, "cid.recv", cid.Recv, "cid.peer", cid.Peer.Hash())
			return nil, fmt.Errorf("connection timed out")
		}
	}
}

func (s *UtpSocket) awaitConnected(
	stream *UtpStream,
	accept *Accept,
	connected chan error,
) {
	s.logger.Debug("waiting for answering the new connections", "dst.peer", accept.cid.Peer.Hash(), "cid.Send", accept.cid.Send, "cid.Recv", accept.cid.Recv)
	err, ok := <-connected
	if err == nil && ok {
		s.logger.Debug("new connection created", "src", accept.cid.Peer.Hash(), "src.cid.Send", accept.cid.Send, "src.cid.Recv", accept.cid.Recv)
		accept.stream <- &StreamResult{stream: stream}
		return
	} else if err != nil {
		s.logger.Debug("connected failed", "peer", accept.cid.Peer.Hash(), "cid.Send", accept.cid.Send, "cid.Recv", accept.cid.Recv)
		accept.stream <- &StreamResult{err: fmt.Errorf("connection failed")}
		return
	}

	s.logger.Debug("connected failed", "peer", accept.cid.Peer.Hash(), "cid.Send", accept.cid.Send, "cid.Recv", accept.cid.Recv)
	accept.stream <- &StreamResult{err: fmt.Errorf("connection aborted")}
	return
}

func (s *UtpSocket) selectAcceptHelper(
	ctx context.Context,
	cid *ConnectionId,
	syn *Packet,
	accept *Accept,
	socketEvents chan *SocketEvent,
) {
	if _, exists := s.getConnStream(cid.Hash()); exists {
		accept.stream <- &StreamResult{
			stream: nil,
			err:    fmt.Errorf("connection ID unavailable"),
		}
		return
	}

	connected := make(chan error, 1)
	streamEvents := make(chan *StreamEvent, 10)

	s.logger.Debug("put a conn stream at selectAcceptHelper", "cid.Peer", cid.Peer, "cid", cid)
	s.putConnStream(cid.Hash(), streamEvents)

	stream := NewUtpStream(
		ctx,
		s.logger,
		cid,
		accept.config,
		syn,
		socketEvents,
		streamEvents,
		connected,
	)

	go s.awaitConnected(stream, accept, connected)
}

func (s *UtpSocket) removeConnStream(key string) {
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	s.logger.Debug("remove conn stream", "key", key)
	delete(s.conns, key)
}

func (s *UtpSocket) putConnStream(key string, streamCh chan *StreamEvent) {
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	s.logger.Debug("put conn stream", "key", key)
	s.conns[key] = streamCh
}

func (s *UtpSocket) getConnStream(key string) (chan *StreamEvent, bool) {
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	ch, ok := s.conns[key]
	return ch, ok
}

func (s *UtpSocket) getConnStreamWithCids(peerInitCid *ConnectionId, ourInitCid *ConnectionId, accCid *ConnectionId) chan *StreamEvent {
	s.logger.Debug("will get conn stream after racing the lock")
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()

	s.logger.Debug("get conn stream", "peerInitCidKey", peerInitCid.Hash())
	if ch, exist := s.conns[peerInitCid.Hash()]; exist {
		return ch
	}
	s.logger.Debug("get conn stream", "ourInitCidKey", ourInitCid.Hash())
	if ch, exist := s.conns[ourInitCid.Hash()]; exist {
		return ch
	}
	s.logger.Debug("get conn stream", "accInitCidKey", accCid.Hash())
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
