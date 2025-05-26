package utp_go

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

const (
	Initiator EndpointType = iota
	Acceptor
)

const (
	ConnConnecting ConnStateType = iota
	ConnConnected
	ConnClosed
)

const DefaultMaxIdleTimeout = 60 * time.Second
const DefaultWindowSize = 1024 * 1024
const DefaultBufferSize = 1024 * 1024

var (
	ErrEmptyDataPayload  = errors.New("empty data payload")
	ErrConnInvalidAckNum = errors.New("invalid ack number")
	ErrInvalidFin        = errors.New("invalid fin")
	ErrInvalidSeqNum     = errors.New("invalid seq number")
	ErrInvalidSyn        = errors.New("invalid syn")
	ErrReset             = errors.New("reset")
	ErrSynFromAcceptor   = errors.New("syn from acceptor")
	ErrTimedOut          = errors.New("timed out")
)

type EndpointType int

type Endpoint struct {
	Type     EndpointType
	SynNum   uint16
	SynAck   uint16
	Attempts int
}

type ClosingRecord struct {
	LocalFin  *uint16
	RemoteFin *uint16
}

type ConnStateType int

type ConnState struct {
	stateType   ConnStateType
	connectedCh chan error
	RecvBuf     *receiveBuffer
	SendBuf     *sendBuffer
	SentPackets *sentPackets
	closing     *ClosingRecord
	Err         error
}

func NewConnState(connected chan error) *ConnState {
	return &ConnState{
		stateType:   ConnConnecting,
		connectedCh: connected,
	}
}

type ConnectionConfig struct {
	MaxPacketSize   uint16
	MaxConnAttempts int
	MaxIdleTimeout  time.Duration
	InitialTimeout  time.Duration
	MinTimeout      time.Duration
	MaxTimeout      time.Duration
	TargetDelay     time.Duration
	WindowSize      uint32
	BufferSize      int
}

func NewConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		MaxConnAttempts: 6,
		MaxIdleTimeout:  DefaultMaxIdleTimeout,
		MaxPacketSize:   defaultMaxPacketSizeBytes,
		InitialTimeout:  defaultInitialTimeout,
		MinTimeout:      defaultMinTimeout,
		MaxTimeout:      defaultMaxTimeout,
		TargetDelay:     defaultTargetMicros,
		WindowSize:      DefaultWindowSize,
		BufferSize:      DefaultBufferSize,
	}
}

func fromConnConfig(config *ConnectionConfig) *ctrlConfig {
	ctrlConfigPtr := defaultCtrlConfig()
	ctrlConfigPtr.MaxPacketSizeBytes = uint32(config.MaxPacketSize)
	ctrlConfigPtr.InitialTimeout = config.InitialTimeout
	ctrlConfigPtr.MinTimeout = config.MinTimeout
	ctrlConfigPtr.MaxTimeout = config.MaxTimeout
	ctrlConfigPtr.TargetDelayMicros = uint32(config.TargetDelay.Microseconds())
	ctrlConfigPtr.WindowSize = config.WindowSize
	return ctrlConfigPtr
}

type queuedWrite struct {
	data     []byte
	written  int
	resultCh chan *readOrWriteResult
}

type readOrWriteResult struct {
	Err  error
	Len  int
	Data []byte
}

type connection struct {
	ctx              context.Context
	logger           log.Logger
	state            *ConnState
	cid              *ConnectionId
	config           *ConnectionConfig
	endpoint         *Endpoint
	peerTsDiff       time.Duration
	peerRecvWindow   uint32
	socketEvents     chan *socketEvent
	unacked          *timeWheel[*packet]
	unackTimeoutCh   chan *packet
	handleExpiration expireFunc[*packet]
	reads            chan *readOrWriteResult
	readable         chan struct{}
	pendingWrites    []*queuedWrite
	writable         chan struct{}
	latestTimeout    *time.Time
	synState         *packet
}

func newConnection(
	ctx context.Context,
	logger log.Logger,
	cid *ConnectionId,
	config *ConnectionConfig,
	syn *packet,
	connected chan error,
	socketEvents chan *socketEvent,
	reads chan *readOrWriteResult,
) *connection {
	var endpoint *Endpoint
	var peerTsDiff time.Duration
	var peerRecvWindow uint32

	if syn != nil {
		synAck := RandomUint16()
		endpoint = &Endpoint{
			Type:   Acceptor,
			SynNum: syn.Header.SeqNum,
			SynAck: synAck,
		}

		now := time.Now().UnixMicro()
		peerTsDiff = time.Duration(now-syn.Header.Timestamp) * time.Microsecond
		peerRecvWindow = syn.Header.WndSize
	} else {
		synNum := RandomUint16()
		endpoint = &Endpoint{
			Type:     Initiator,
			SynNum:   synNum,
			Attempts: 0,
		}
		peerTsDiff = 0
		peerRecvWindow = math.MaxUint32
	}

	unackTimeoutCh := make(chan *packet, 1000)
	handleExpiration := func(key any, pkt *packet) {
		select {
		case unackTimeoutCh <- pkt:
		case <-ctx.Done():
		}
	}

	return &connection{
		ctx:            ctx,
		logger:         logger,
		state:          NewConnState(connected),
		cid:            cid,
		config:         config,
		endpoint:       endpoint,
		peerTsDiff:     peerTsDiff,
		peerRecvWindow: peerRecvWindow,
		socketEvents:   socketEvents,
		unacked:        newTimeWheel[*packet](config.InitialTimeout/4, 8, handleExpiration),
		unackTimeoutCh: unackTimeoutCh,
		reads:          reads,
		readable:       make(chan struct{}, 3),
		pendingWrites:  make([]*queuedWrite, 0),
		writable:       make(chan struct{}, 3),
		latestTimeout:  nil,
	}
}

func (c *connection) eventLoop(stream *UtpStream) error {
	defer c.unacked.stop()
	if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("uTP conn starting", "dst.peer", c.cid.Peer, "cid.Send", c.cid.Send, "cid.Recv", c.cid.Recv)
	}
	// Initialize connection based on endpoint type
	if c.endpoint.Type == Initiator {
		synSeqNum := c.endpoint.SynNum
		synPkt := c.synPacket(synSeqNum)
		c.socketEvents <- newOutgoingSocketEvent(synPkt, c.cid)
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("put a initial syn packet to delay map", "socketEvents.len", len(c.socketEvents), "dst.peer", c.cid.Peer, "synSeqNum", synSeqNum)
		}
		c.unacked.put(synSeqNum, synPkt, c.config.InitialTimeout)

		c.endpoint.Attempts = 1
	} else {
		// Acceptor connection
		syn := c.endpoint.SynNum
		synAck := c.endpoint.SynAck

		c.synState = c.statePacket()

		c.logger.Debug("a initial state packet", "peer", c.cid.Peer, "cid.Send", c.cid.Send, "cid.Recv", c.cid.Recv)
		c.socketEvents <- newOutgoingSocketEvent(c.synState, c.cid)

		recvBuf := newReceiveBufferWithLogger(c.config.BufferSize, syn, c.logger)
		sendBuf := newSendBuffer(c.config.BufferSize)
		congestionCtrl := newDefaultController(fromConnConfig(c.config))
		sentPacketsHolder := newSentPackets(synAck-1, congestionCtrl, c.logger) // wrapping subtraction

		if c.state != nil && c.state.connectedCh != nil {
			c.state.connectedCh <- nil
		} else {
			panic("connection in invalid statePacket prior to event loop beginning")
		}

		c.state.stateType = ConnConnected
		c.state.RecvBuf = recvBuf
		c.state.SendBuf = sendBuf
		c.state.SentPackets = sentPacketsHolder
	}

	idleTimer := time.NewTimer(c.config.MaxIdleTimeout)
	resetIdleTimer := func() {
		idleTimer.Reset(c.config.MaxIdleTimeout)
	}
	defer idleTimer.Stop()
	handleIncoming := func(event *streamEvent) {
		if event.Type == streamIncoming {
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("incoming packet",
					"stream.streamEvents.len", len(stream.streamEvents),
					"src.peer", c.cid.Peer,
					"packet.type", event.Packet.Header.PacketType.String(),
					"packet.seqNum", event.Packet.Header.SeqNum,
					"packet.ackNum", event.Packet.Header.AckNum,
					"buf.len", len(event.Packet.Body))
			}
			// reset idle timeout
			resetIdleTimer()
			c.onPacket(event.Packet, time.Now())
		} else if event.Type == streamShutdown {
			stream.shutdown.Store(true)
		}
	}

	handleWrites := func(write *queuedWrite, ok bool) {
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("get queued write from writes", "dst.peer", c.cid.Peer, "content", len(write.data))
		}
		resetIdleTimer()
		c.onWrite(write)
	}

	handleTimeout := func(timeoutPkt *packet) {
		c.logger.Debug("unack timeout",
			"seq", timeoutPkt.Header.SeqNum,
			"ack", timeoutPkt.Header.AckNum,
			"item.key", timeoutPkt.Header.SeqNum,
			"type", timeoutPkt.Header.PacketType.String())
		c.onTimeout(timeoutPkt, time.Now())
	}

	handleIdleTimeout := func() {
		if c.state.stateType != ConnClosed {
			c.logger.Trace("idle timeout, closing...")
			c.state.stateType = ConnClosed
			c.state.Err = ErrTimedOut
		}
	}

	handleCtxDone := func() {
		if !stream.shutdown.Load() {
			c.logger.Trace("ctx done, uTP conn initiating shutdown...", "err", c.ctx.Err())
			stream.shutdown.Store(true)
		}
	}

	var maxStreamEventLen int
	for {
		maxStreamEventLen = max(maxStreamEventLen, len(stream.streamEvents))
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("connection event count",
				"maxStreamEventLen", maxStreamEventLen,
				"streamEvents.len", len(stream.streamEvents),
				"readable.len", len(c.readable),
				"writeable.len", len(c.writable))
		}
		select {
		case event := <-stream.streamEvents:
			handleIncoming(event)
			goto afterSelect
		default:
		}
		select {
		case event := <-stream.streamEvents:
			handleIncoming(event)
		case write, ok := <-stream.writes:
			handleWrites(write, ok)
		case <-c.readable:
			c.processReads()
		case <-c.writable:
			c.processWrites(time.Now())
		case timeoutPkt := <-c.unackTimeoutCh:
			handleTimeout(timeoutPkt)
		case <-idleTimer.C:
			handleIdleTimeout()
		case <-c.ctx.Done():
			handleCtxDone()
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("stream context done, will force stop...", "c.cid.peer", c.cid.Peer, "c.cid.Send", c.cid.Send, "c.cid.Recv", c.cid.Recv)
			}
			return c.ctx.Err()
		}
	afterSelect:
		if stream.shutdown.Load() && c.state.stateType != ConnClosed {
			c.shutdown()
		}

		if c.state.stateType == ConnClosed {
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("uTP conn closing...", "err", c.state.Err, "c.cid.Send", c.cid.Send, "c.cid.Recv", c.cid.Recv)
			}
			if !c.eof() {
				c.processReads()
			}
			c.processWrites(time.Now())
			if c.state.RecvBuf != nil {
				c.state.RecvBuf.close()
			}
			c.socketEvents <- newShutdownSocketEvent(c.cid)
			return c.state.Err
		}
	}
}

func (c *connection) shutdown() {
	switch c.state.stateType {
	case ConnConnecting:
		c.state.stateType = ConnClosed
	case ConnClosed:
		// ignore
	case ConnConnected:
		if c.state.closing != nil {
			localFin := c.state.closing.LocalFin
			// If we have not sent our FIN, and there are no pending writes, and there is no
			// pending data in the send buffer, then send our FIN
			if localFin == nil && len(c.pendingWrites) == 0 && c.state.SendBuf.IsEmpty() {
				recvWindow := uint32(c.state.RecvBuf.Available())
				seqNum := c.state.SentPackets.NextSeqNum()
				ackNum := c.state.RecvBuf.AckNum()
				selectiveAck := c.state.RecvBuf.SelectiveAck()

				fin := NewPacketBuilder(
					st_fin,
					c.cid.Send,
					NowMicro(),
					recvWindow,
					seqNum,
				).WithAckNum(ackNum).WithSelectiveAck(selectiveAck).Build()

				c.state.closing.LocalFin = &seqNum
				if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
					c.logger.Trace("transmitting FIN", "dst.Peer", c.cid.Peer, "dst.Send", c.cid.Send, "dst.Recv", c.cid.Recv, "seq", seqNum)
				}
				c.transmit(fin, time.Now())
			}
		} else {
			var localFin *uint16
			if len(c.pendingWrites) == 0 && c.state.SendBuf.IsEmpty() {
				recvWindow := uint32(c.state.RecvBuf.Available())
				seqNum := c.state.SentPackets.NextSeqNum()
				ackNum := c.state.RecvBuf.AckNum()
				selectiveAck := c.state.RecvBuf.SelectiveAck()

				fin := NewPacketBuilder(
					st_fin,
					c.cid.Send,
					NowMicro(),
					recvWindow,
					seqNum,
				).WithAckNum(ackNum).
					WithSelectiveAck(selectiveAck).
					Build()

				localFin = &seqNum
				if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
					c.logger.Trace("transmitting FIN", "dst.Peer", c.cid.Peer, "dst.Send", c.cid.Send, "dst.Recv", c.cid.Recv, "seq", seqNum)
				}
				c.transmit(fin, time.Now())
			}
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("init localFin of closingRecord", "dst.peer", c.cid.Peer, "dst.send", c.cid.Send, "dst.recv", c.cid.Recv, "localFin", localFin)
			}
			c.state.closing = &ClosingRecord{
				LocalFin:  localFin,
				RemoteFin: nil,
			}
		}
	}
}

func (c *connection) processWrites(now time.Time) {
	switch c.state.stateType {
	case ConnConnecting:
		return
	case ConnClosed:
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Warn("connection is closed, will not process pending writes",
				"c.cid.send", c.cid.Send, "c.cid.recv", c.cid.Recv)
		}
		result := &readOrWriteResult{
			Err: c.state.Err,
		}
		for _, w := range c.pendingWrites {
			w.resultCh <- result
		}
		return
	default:
	}

	// Compose data packets
	windowSize := minUint32(c.state.SentPackets.Window(), c.peerRecvWindow)
	var payloads [][]byte

	for windowSize > 0 {
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("has window size to send a packet data in sendBuffer", "windowSize", windowSize)
		}
		maxDataSize := minUint32(windowSize, uint32(c.config.MaxPacketSize-64))
		data := make([]byte, maxDataSize)
		n := c.state.SendBuf.Read(data)
		if n == 0 {
			break
		}
		payloads = append(payloads, data[:n])
		windowSize -= uint32(n)
	}

	// Write pending data to send buffer
	for len(c.pendingWrites) > 0 {
		bufSpace := c.state.SendBuf.Available()
		if bufSpace <= 0 {
			break
		}

		writeReq := c.pendingWrites[0]

		if len(writeReq.data) <= bufSpace {
			c.state.SendBuf.Write(writeReq.data)
			result := &readOrWriteResult{
				Len: len(writeReq.data) + writeReq.written,
			}
			writeReq.resultCh <- result
			c.pendingWrites = c.pendingWrites[1:]
		} else {
			nextWrite := writeReq.data[:bufSpace]
			remainingData := writeReq.data[bufSpace:]
			c.state.SendBuf.Write(nextWrite)

			writeReq.data = remainingData
			writeReq.written += bufSpace
		}
		select {
		case c.writable <- struct{}{}:
		default:
		}
	}

	// transmit data packets
	seqNum := c.state.SentPackets.NextSeqNum()
	recvWindow := uint32(c.state.RecvBuf.Available())
	ackNum := c.state.RecvBuf.AckNum()
	selectiveAck := c.state.RecvBuf.SelectiveAck()

	for _, payload := range payloads {
		packetInst := NewPacketBuilder(
			st_data,
			c.cid.Send,
			NowMicro(),
			recvWindow,
			seqNum,
		).WithPayload(payload).WithTsDiffMicros(uint32(c.peerTsDiff.Microseconds())).WithAckNum(ackNum).WithSelectiveAck(selectiveAck).Build()

		c.transmit(packetInst, now)
		seqNum = seqNum + 1 // wrapping add in uint16
	}
}

func (c *connection) onWrite(writeReq *queuedWrite) {
	writeReq.written = 0
	switch c.state.stateType {
	case ConnConnecting:
		// There are 0 bytes written so far
		c.pendingWrites = append(c.pendingWrites, writeReq)

	case ConnConnected:
		if c.state.closing != nil {
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("append a queuedWrite to pending writes when closing the conn...")
			}
			if c.state.closing.LocalFin == nil && c.state.closing.RemoteFin != nil {
				c.pendingWrites = append(c.pendingWrites, writeReq)
			} else {
				writeReq.resultCh <- &readOrWriteResult{Len: 0}
			}
		} else {
			c.logger.Debug("append a queuedWrite to pending writes")
			c.pendingWrites = append(c.pendingWrites, writeReq)
		}

	case ConnClosed:
		c.logger.Warn("discard a queuedWrite when closed the conn...")
		result := &readOrWriteResult{
			Err: c.state.Err,
			Len: 0,
		}
		writeReq.resultCh <- result
	}
	c.processWrites(time.Now())
	select {
	case c.writable <- struct{}{}:
	default:
	}
}

func (c *connection) processReads() {
	var recvBuf *receiveBuffer
	switch c.state.stateType {
	case ConnConnecting:
		return
	case ConnConnected:
		recvBuf = c.state.RecvBuf
	case ConnClosed:
		result := &readOrWriteResult{
			Err: c.state.Err,
		}
		c.reads <- result
		c.logger.Debug("read eof...")
		return
	}

	currentTime := time.Now()
	if recvBuf != nil && c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("read data saving in the recvBuf, start...", "available", recvBuf.Available(), "isEmpty", recvBuf.IsEmpty())
	}
	for recvBuf != nil && !recvBuf.IsEmpty() {
		buf := make([]byte, c.config.MaxPacketSize)
		n := recvBuf.Read(buf)
		if n == 0 {
			break
		}
		c.reads <- &readOrWriteResult{Data: buf, Len: n}
	}
	if recvBuf != nil && c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("read data saving in the recvBuf, end...", "duration", time.Since(currentTime), "available", recvBuf.Available(), "isEmpty", recvBuf.IsEmpty())
	}

	// If we have reached eof, send an empty resultCh to all pending reads
	if c.eof() {
		c.reads <- &readOrWriteResult{Data: make([]byte, 0)}
	}
}

func (c *connection) eof() bool {
	switch c.state.stateType {
	case ConnConnecting:
		return false
	case ConnConnected:
		if c.state.closing != nil && c.state.closing.RemoteFin != nil {
			return c.state.RecvBuf.AckNum() == *c.state.closing.RemoteFin
		}
		return false
	case ConnClosed:
		return true
	default:
		return false
	}
}

func (c *connection) onTimeout(originPacket *packet, now time.Time) {
	switch c.state.stateType {
	case ConnConnecting:
		if c.endpoint.Type == Acceptor {
			return
		}
		if c.endpoint.Attempts >= c.config.MaxConnAttempts {
			c.logger.Error("quitting connection attempt", "attempts", c.endpoint.Attempts)
			err := ErrTimedOut
			if c.state.connectedCh != nil {
				c.state.connectedCh <- err
			}
			c.state.stateType = ConnClosed
			c.state.Err = err
			return
		} else {
			seq := c.endpoint.SynNum
			logMsg := fmt.Sprintf("retrying connection, after %d attempts", c.endpoint.Attempts)
			switch c.endpoint.Attempts {
			case 1, 0:
				c.logger.Trace(logMsg)
			case 2:
				c.logger.Debug(logMsg)
			case 3:
				c.logger.Info(logMsg)
			default:
				c.logger.Warn(logMsg)
			}
			c.endpoint.Attempts += 1

			// Double previous timeout for exponential backoff on each attempt
			timeout := c.config.InitialTimeout * time.Duration(math.Pow(1.5, float64(c.endpoint.Attempts)))

			c.unacked.put(seq, originPacket, timeout)

			// Re-send SYN packet
			c.socketEvents <- newOutgoingSocketEvent(c.synPacket(seq), c.cid)
		}

	case ConnConnected:
		// If the timed out packet is a SYN, do nothing
		if originPacket.Header.PacketType == st_syn {
			return
		}

		// Handle timeout amplification prevention
		var isTimeout bool
		if c.latestTimeout != nil {
			isTimeout = time.Since(*c.latestTimeout) > c.state.SentPackets.Timeout()
		} else {
			isTimeout = true
		}

		if isTimeout {
			c.state.SentPackets.OnTimeout()
			currentTime := time.Now()
			c.latestTimeout = &currentTime
		}

		retransmissionPacket := &packet{
			Header: &PacketHeaderV1{
				PacketType:   originPacket.Header.PacketType,
				Version:      originPacket.Header.Version,
				Extension:    originPacket.Header.Extension,
				ConnectionId: originPacket.Header.ConnectionId,
				SeqNum:       originPacket.Header.SeqNum,
			},
			Body: originPacket.Body,
			Eack: nil,
		}

		recvWindow := uint32(c.state.RecvBuf.Available())
		nowMicros := time.Now().UnixMicro()
		tsDiffMicros := uint32(c.peerTsDiff.Microseconds())

		retransmissionPacket.Header.WndSize = recvWindow
		retransmissionPacket.Header.Timestamp = nowMicros
		retransmissionPacket.Header.TimestampDiff = tsDiffMicros
		retransmissionPacket.Header.AckNum = c.state.RecvBuf.AckNum()
		retransmissionPacket.Eack = c.state.RecvBuf.SelectiveAck()

		//newPacket := NewPacketBuilder(packet.Header.PacketType, packet.Header.ConnectionId, uint32(nowMicros), recvWindow, packet.Header.SeqNum).
		//	WithAckNum(c.state.RecvBuf.AckNum()).
		//	WithSelectiveAck(c.state.RecvBuf.SelectiveAck()).
		//	WithTsDiffMicros(tsDiffMicros).
		//	WithPayload(packet.Body).
		//	Build()

		c.transmit(retransmissionPacket, now)
	default:
	}
}

func (c *connection) onPacket(packet *packet, now time.Time) {
	if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("on packet start...",
			"packet.body.len", len(packet.Body),
			"packet.type", packet.Header.PacketType.String(),
			"packet.cid", packet.Header.ConnectionId,
			"packet.seqnum", packet.Header.SeqNum,
			"packet.acknum", packet.Header.AckNum,
			"packet.windowSize", packet.Header.WndSize,
			"now", now)
	}
	nowMicros := time.Now().UnixMicro()
	c.peerRecvWindow = packet.Header.WndSize

	// Cap the diff to handle clock differences between machines
	peerTsDiff := time.Microsecond * time.Duration(nowMicros-int64(packet.Header.TimestampDiff))
	if peerTsDiff > c.config.MaxIdleTimeout {
		c.peerTsDiff = time.Second
	} else {
		c.peerTsDiff = peerTsDiff
	}

	// Handle different packet types
	var err error
	switch packet.Header.PacketType {
	case st_syn:
		c.onSyn(packet.Header.SeqNum)
	case st_state:
		c.onState(packet.Header.SeqNum, packet.Header.AckNum)
	case st_data:
		err = c.onData(packet.Header.SeqNum, packet.Body)
	case st_fin:
		err = c.onFin(packet.Header.SeqNum, packet.Body)
	case st_reset:
		c.onReset()
	}
	if err != nil {
		c.logger.Warn("on packet handle data or fin err", "err", err)
	}

	// Process acknowledgments
	switch packet.Header.PacketType {
	case st_state, st_data, st_fin:
		delay := time.Duration(packet.Header.TimestampDiff) * time.Microsecond
		if err = c.processAck(packet.Header.AckNum, packet.Eack, delay, now); err != nil {
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("ack does not correspond to known seq_num",
					"packet.type", packet.Header.PacketType,
					"packet.seqNum", packet.Header.SeqNum,
					"packet.ackNum", packet.Header.AckNum,
					"err", err)
			}
		}
	default:
	}

	// Handle retransmissions
	c.retransmitLostPackets(now)

	// Send STATE packet if appropriate
	switch packet.Header.PacketType {
	case st_syn:
		if c.synState == nil {
			// Teh synState is generated at the beginning of the eventLoop
			c.logger.Warn("missing SYN STATE, ")
			if statePacket := c.statePacket(); statePacket != nil {
				c.synState = statePacket
			}
		}

		c.socketEvents <- newOutgoingSocketEvent(c.synState, c.cid)

	case st_data, st_fin:
		if statePacket := c.statePacket(); statePacket != nil {
			if c.logger.Enabled(BASE_CONTEXT, log.LevelDebug) {
				c.logger.Debug("create a state packet to send out",
					"packet.seqNum", statePacket.Header.SeqNum,
					"packet.ackNum", statePacket.Header.AckNum,
					"packet.cid", statePacket.Header.ConnectionId)
			}

			c.socketEvents <- newOutgoingSocketEvent(statePacket, c.cid)
		}
	}

	// Notify writable on STATE packets
	if packet.Header.PacketType == st_state {
		select {
		case c.writable <- struct{}{}:
		default:
		}
	}

	// Notify readable on data or FIN
	if len(packet.Body) > 0 || packet.Header.PacketType == st_fin {
		select {
		case c.readable <- struct{}{}:
		default:
		}
	}

	// Handle connection closing cases
	if c.state.stateType == ConnConnected && c.state.closing != nil && c.state.closing.LocalFin != nil {
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("try close connection locally...", "dst.Peer", c.cid.Peer, "dst.Send", c.cid.Send, "dst.Recv", c.cid.Recv)
		}
		lastAckNum, isNone := c.state.SentPackets.LastAckNum()
		if !isNone && lastAckNum == *c.state.closing.LocalFin {
			c.state.stateType = ConnClosed
			c.state.Err = nil
		}
	}

	if c.state.stateType == ConnConnected && c.state.closing != nil && c.state.closing.RemoteFin != nil {
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("close connection remotely...", "dst.Peer", c.cid.Peer, "dst.Send", c.cid.Send, "dst.Recv", c.cid.Recv)
		}
		if !c.state.SentPackets.HasUnackedPackets() && c.state.RecvBuf.AckNum() == *c.state.closing.RemoteFin {
			c.processReads()
			c.state.stateType = ConnClosed
			c.state.Err = nil
		}
	}

	if c.state.stateType == ConnConnected && c.state.closing != nil && c.state.closing.RemoteFin != nil && c.state.closing.LocalFin != nil {
		c.state.stateType = ConnClosed
		c.state.Err = nil
	}
}

func (c *connection) processAck(
	ackNum uint16,
	selectiveAck *SelectiveAck,
	delay time.Duration,
	now time.Time,
) error {
	if c.state.stateType != ConnConnected {
		return nil
	}

	fullAcked, selectedAcks, err := c.state.SentPackets.onAck(
		ackNum, selectiveAck, delay, now)
	if err != nil {
		seqRange := c.state.SentPackets.SeqNumRange()
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("sent packets onAck has error",
				"err", err,
				"cid.send", c.cid.Send,
				"cid.recv", c.cid.Recv,
				"ackNum", ackNum,
				"seqStart",
				seqRange.start,
				"seqEnd", seqRange.end)
		}
		if errors.Is(err, ErrInvalidAckNum) {
			c.reset(err)
			return err
		}
		return err
	}
	if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("process ack",
			"cid.send", c.cid.Send,
			"cid.recv", c.cid.Recv,
			"fullAcked.start", fullAcked.start,
			"fullAcked.end", fullAcked.end)
	}
	c.unacked.retain(func(key any) bool {
		// return true to remove
		return fullAcked.Contains(key.(uint16))
	})
	for _, selectedAck := range selectedAcks {
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("process ack, will remove acked num from innerMap",
				"cid.send", c.cid.Send,
				"cid.recv", c.cid.Recv,
				"ackNum", selectedAck)
		}
		c.unacked.remove(selectedAck)
	}

	return nil
}

func (c *connection) onSyn(seqNum uint16) {
	var err error

	if c.endpoint.Type == Acceptor {
		// If we are the accepting endpoint, check whether the SYN is a retransmission
		// A non-matching sequence number is incorrect behavior
		if seqNum != c.endpoint.SynNum {
			err = ErrInvalidSyn
		}
	} else {
		// If we are the initiating endpoint, then an incoming SYN is incorrect behavior
		err = ErrSynFromAcceptor
	}

	if err != nil {
		if c.state.stateType != ConnClosed {
			c.reset(err)
		}
	}
}

func (c *connection) onState(seqNum, ackNum uint16) {
	if ConnConnecting != c.state.stateType {
		return
	}

	if c.endpoint.Type == Initiator && ackNum == c.endpoint.SynNum {
		// NOTE: In a deviation from the specification, we initialize the ACK num
		// to the sequence number of the SYN-ACK minus 1. This is consistent with
		// the reference implementation and the libtorrent implementation.
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("connect success, will initial connection state...",
				"cid.send", c.cid.Send, "cid.recv", c.cid.Recv)
		}
		recvBuf := newReceiveBufferWithLogger(c.config.BufferSize, seqNum-1, c.logger) // wrapping subtraction for uint16
		sendBuf := newSendBuffer(c.config.BufferSize)

		congestionCtrl := newDefaultController(fromConnConfig(c.config))
		sentPacketsHolder := newSentPackets(c.endpoint.SynNum, congestionCtrl, c.logger)

		close(c.state.connectedCh)
		c.state.stateType = ConnConnected
		c.state.RecvBuf = recvBuf
		c.state.SendBuf = sendBuf
		c.state.SentPackets = sentPacketsHolder
	}
}

func (c *connection) onData(seqNum uint16, data []byte) error {
	if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("on data packet", "seqNum", seqNum, "data.len", len(data))
	}
	// If the data payload is empty, then reset the connection
	if len(data) == 0 {
		c.reset(ErrEmptyDataPayload)
	}

	switch c.state.stateType {
	case ConnConnecting:
		if c.endpoint.Type == Acceptor {
			return errors.New("unreachable: connection should be marked established")
		} else {
			// connection being established.
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("connection being established, ignore", "src.peer", c.cid.Peer, "seqNum", seqNum)
			}
			return nil
		}

	case ConnConnected:
		if c.state.closing != nil && c.state.closing.RemoteFin != nil {
			// connection is closing
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("connection has received remote FIN", "src.peer", c.cid.Peer, "seqNum", seqNum)
			}
			start := c.state.RecvBuf.InitSeqNum()
			seqRange := newCircularRangeInclusive(start, *c.state.closing.RemoteFin)
			if !seqRange.Contains(seqNum) {
				c.state.stateType = ConnClosed
				c.state.Err = ErrInvalidSeqNum
				return nil
			}
		}
		// not closing should send data
		if len(data) <= c.state.RecvBuf.Available() {
			err := c.state.RecvBuf.Write(data, seqNum)
			if err != nil {
				c.logger.Warn("write data to recv buffer, but available space is not enough",
					"src.peer", c.cid.Peer, "seqNum", seqNum, "data.len", len(data))
			}
			if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				c.logger.Trace("write data to recv buffer",
					"src.peer", c.cid.Peer, "seqNum", seqNum, "data.len", len(data), "nextAckNum", c.state.RecvBuf.AckNum())
			}
			return err
		}
	default:
		// do nothing
	}
	return nil
}

func (c *connection) onFin(seqNum uint16, data []byte) error {
	switch c.state.stateType {
	case ConnConnecting, ConnClosed:
		return nil

	case ConnConnected:
		if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
			c.logger.Trace("received FIN", "seq", seqNum, "c.cid.send", c.cid.Send, "c.cid.recv", c.cid.Recv)
		}
		if c.state.closing != nil {
			if c.state.closing.RemoteFin != nil {
				// If we have already received a FIN, a subsequent FIN with a different
				// sequence number is incorrect behavior
				if seqNum != *c.state.closing.RemoteFin {
					c.reset(ErrInvalidFin)
				}
			} else {
				remoteFin := seqNum
				c.state.closing.RemoteFin = &remoteFin
				return c.state.RecvBuf.Write(data, seqNum)
			}
		} else {
			// Register the FIN with the receive buffer
			if err := c.state.RecvBuf.Write(data, seqNum); err != nil {
				return err
			}
			c.state.closing = &ClosingRecord{
				LocalFin:  nil,
				RemoteFin: &seqNum,
			}
		}
		return nil
	default:
		return nil
	}
}

func (c *connection) onReset() {
	c.logger.Warn("RESET from remote")

	// If the connection is not already closed or reset, then reset the connection
	if c.state.stateType != ConnClosed {
		c.reset(ErrReset)
	}
}

func (c *connection) reset(err error) {
	c.logger.Warn("resetting connection", "err", err)

	// If we already sent our fin and got a reset we assume the receiver already got our fin
	// and has successfully closed their connection, hence mark this as a successful close.
	if c.state.stateType == ConnConnected {
		if c.state.closing != nil && c.state.closing.LocalFin != nil {
			c.state.stateType = ConnClosed
			return
		}
	}

	c.state.stateType = ConnClosed
	c.state.Err = err
}

func (c *connection) synPacket(seqNum uint16) *packet {
	nowMicros := time.Now().UnixMicro()
	return NewPacketBuilder(
		st_syn,
		c.cid.Recv,
		uint32(nowMicros),
		c.config.WindowSize,
		seqNum,
	).Build()
}

func (c *connection) statePacket() *packet {
	now := time.Now().UnixMicro()
	tsDiffMicros := uint32(c.peerTsDiff.Microseconds())

	switch c.state.stateType {
	case ConnConnecting:
		if c.endpoint.Type == Initiator {
			return nil
		}

		syn := c.endpoint.SynNum
		synAck := c.endpoint.SynAck
		return NewPacketBuilder(st_state, c.cid.Send, uint32(now), c.config.WindowSize, synAck).
			WithTsDiffMicros(tsDiffMicros).
			WithAckNum(syn).
			Build()

	case ConnConnected:
		// NOTE: Consistent with the reference implementation and the libtorrent
		// implementation, STATE packets always include the next sequence number.
		seqNum := c.state.SentPackets.NextSeqNum()
		ackNum := c.state.RecvBuf.AckNum()
		recvWindow := uint32(c.state.RecvBuf.Available())
		selectiveAck := c.state.RecvBuf.SelectiveAck()

		return NewPacketBuilder(st_state, c.cid.Send, uint32(now), recvWindow, seqNum).
			WithTsDiffMicros(tsDiffMicros).
			WithAckNum(ackNum).
			WithSelectiveAck(selectiveAck).
			Build()

	default: // ClosedState
		return nil
	}
}

func (c *connection) retransmitLostPackets(now time.Time) {
	if c.state.stateType != ConnConnected {
		return
	}
	if !c.state.SentPackets.HasLostPackets() {
		return
	}
	connID := c.cid.Send
	nowMicros := time.Now().UnixMicro()
	recvWindow := uint32(c.state.RecvBuf.Available())
	tsDiffMicros := uint32(c.peerTsDiff.Microseconds())

	for _, lostPacket := range c.state.SentPackets.LostPackets() {
		seqNum := lostPacket.SeqNum
		packetType := lostPacket.PacketType
		payload := lostPacket.Data

		builder := NewPacketBuilder(packetType, connID, uint32(nowMicros), recvWindow, seqNum)
		if payload != nil {
			builder.WithPayload(payload)
		}

		packetInst := builder.
			WithTsDiffMicros(tsDiffMicros).
			WithAckNum(c.state.RecvBuf.AckNum()).
			WithSelectiveAck(c.state.RecvBuf.SelectiveAck()).
			Build()
		if c.logger.Enabled(BASE_CONTEXT, log.LevelDebug) {
			c.logger.Debug("will retransmit lost packet",
				"packet.type", packetInst.Header.PacketType,
				"packet.seqNum", packetInst.Header.SeqNum,
				"packet.ackNum", packetInst.Header.AckNum,
				"packet.cid", packetInst.Header.ConnectionId,
				"packet.data.len", len(payload))
		}
		c.transmit(packetInst, now)
	}
}

func (c *connection) transmit(packet *packet, now time.Time) {
	var payload []byte
	var length uint32

	if len(packet.Body) > 0 {
		payload = packet.Body
		length = uint32(len(packet.Body))
	}

	if c.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		c.logger.Trace("will transmit packet",
			"cid", packet.Header.ConnectionId,
			"packet.type", packet.Header.PacketType.String(),
			"packet.seqNum", packet.Header.SeqNum,
			"packet.ackNum", packet.Header.AckNum,
			"innerMap.key", packet.Header.SeqNum,
			"packet.body.len", len(packet.Body))
	}

	c.state.SentPackets.OnTransmit(packet.Header.SeqNum, packet.Header.PacketType, payload, length, now)
	c.unacked.put(packet.Header.SeqNum, packet, c.state.SentPackets.Timeout())

	c.socketEvents <- newOutgoingSocketEvent(packet, c.cid)
}
