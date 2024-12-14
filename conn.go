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
	RecvBuf     *ReceiveBuffer
	SendBuf     *SendBuffer
	SentPackets *SentPackets
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

func FromConnConfig(config *ConnectionConfig) *CtrlConfig {
	ctrlConfig := DefaultCtrlConfig()
	ctrlConfig.MaxPacketSizeBytes = uint32(config.MaxPacketSize)
	ctrlConfig.InitialTimeout = config.InitialTimeout
	ctrlConfig.MinTimeout = config.MinTimeout
	ctrlConfig.MaxTimeout = config.MaxTimeout
	ctrlConfig.TargetDelayMicros = uint32(config.TargetDelay.Microseconds())
	ctrlConfig.WindowSize = config.WindowSize
	return ctrlConfig
}

type QueuedWrite struct {
	data     []byte
	written  int
	resultCh chan *ReadOrWriteResult
}

type ReadOrWriteResult struct {
	Err  error
	Len  int
	Data []byte
}

type Connection struct {
	ctx            context.Context
	logger         log.Logger
	state          *ConnState
	cid            *ConnectionId
	config         *ConnectionConfig
	endpoint       *Endpoint
	peerTsDiff     time.Duration
	peerRecvWindow uint32
	socketEvents   chan *SocketEvent
	unacked        *DelayMap[*Packet]
	reads          chan *ReadOrWriteResult
	readable       chan struct{}
	pendingWrites  []*QueuedWrite
	writable       chan struct{}
	latestTimeout  *time.Time
}

func NewConnection(
	ctx context.Context,
	logger log.Logger,
	cid *ConnectionId,
	config *ConnectionConfig,
	syn *Packet,
	connected chan error,
	socketEvents chan *SocketEvent,
	reads chan *ReadOrWriteResult,
) *Connection {
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

	return &Connection{
		ctx:            ctx,
		logger:         logger,
		state:          NewConnState(connected),
		cid:            cid,
		config:         config,
		endpoint:       endpoint,
		peerTsDiff:     peerTsDiff,
		peerRecvWindow: peerRecvWindow,
		socketEvents:   socketEvents,
		unacked:        NewDelayMap[*Packet](),
		reads:          reads,
		readable:       make(chan struct{}, 100),
		pendingWrites:  make([]*QueuedWrite, 0),
		writable:       make(chan struct{}, 100),
		latestTimeout:  nil,
	}
}

func (c *Connection) EventLoop(stream *UtpStream) error {
	c.logger.Debug("uTP conn starting", "dst.peer", c.cid.Peer, "cid.Send", c.cid.Send, "cid.Recv", c.cid.Recv)

	// Initialize connection based on endpoint type
	if c.endpoint.Type == Initiator {
		synSeqNum := c.endpoint.SynNum
		synPkt := c.SynPacket(synSeqNum)
		select {
		case c.socketEvents <- &SocketEvent{
			Type:         Outgoing,
			Packet:       synPkt,
			ConnectionId: c.cid,
		}:
		}
		c.logger.Debug("put a initial syn packet to delay map", "dst.peer", c.cid.Peer, "synSeqNum", synSeqNum)
		c.unacked.Put(synSeqNum, synPkt, c.config.InitialTimeout)

		c.endpoint.Attempts = 1
	} else {
		syn := c.endpoint.SynNum
		synAck := c.endpoint.SynAck

		statePacket := c.StatePacket()
		c.logger.Debug("a initial state packet", "peer", c.cid.Peer, "cid.Send", c.cid.Send, "cid.Recv", c.cid.Recv)
		c.socketEvents <- &SocketEvent{
			Type:         Outgoing,
			Packet:       statePacket,
			ConnectionId: c.cid,
		}

		recvBuf := NewReceiveBuffer(c.config.BufferSize, syn)
		sendBuf := NewSendBuffer(c.config.BufferSize)
		congestionCtrl := NewDefaultController(FromConnConfig(c.config))
		sentPackets := NewSentPackets(synAck-1, congestionCtrl) // wrapping subtraction

		if c.state != nil && c.state.connectedCh != nil {
			c.state.connectedCh <- nil
		} else {
			panic("connection in invalid statePacket prior to event loop beginning")
		}

		c.state.stateType = ConnConnected
		c.state.RecvBuf = recvBuf
		c.state.SendBuf = sendBuf
		c.state.SentPackets = sentPackets
	}

	idleTimer := time.NewTimer(c.config.MaxIdleTimeout)
	resetIdleTimer := func() {
		c.logger.Debug("Reset idle timer")
		idleTimer.Reset(c.config.MaxIdleTimeout)
	}
	defer idleTimer.Stop()

	c.logger.Debug("stream ptr", "dst.peer", c.cid.Peer, "ptr(stream)", fmt.Sprintf("%p", stream), "ptr(stream.writes)", fmt.Sprintf("%p", stream.writes))

	for {
		select {
		case event := <-stream.streamEvents:
			if event.Type == StreamIncoming {
				c.logger.Debug("incoming packet",
					"src.peer", c.cid.Peer,
					"packet.type", event.Packet.Header.PacketType.String(),
					"packet.seqNum", event.Packet.Header.SeqNum,
					"packet.ackNum", event.Packet.Header.AckNum,
					"buf.len", len(event.Packet.Body))
				// Reset idle timeout
				resetIdleTimer()
				c.OnPacket(event.Packet, time.Now())
			} else if event.Type == StreamShutdown {
				stream.shutdown.Store(true)
			}
		case write, ok := <-stream.writes:
			c.logger.Debug("get queued write from writes", "dst.peer", c.cid.Peer, "content", len(write.data))
			if !ok || stream.shutdown.Load() {
				break
			}
			resetIdleTimer()
			c.OnWrite(write)

		case <-c.readable:
			c.logger.Debug("has data to continually reading start")
			c.ProcessReads()
			c.logger.Debug("has data to continually reading end")

		case <-c.writable:
			c.logger.Debug("has data to continually writing start")
			c.ProcessWrites(time.Now())
			c.logger.Debug("has data to continually writing end")

		// todo case handle unacked timeout
		case timeoutItem := <-c.unacked.timeoutCh():
			timeoutPkt := timeoutItem.Item
			c.logger.Debug("unack timeout",
				"seq", timeoutPkt.Header.SeqNum,
				"ack", timeoutPkt.Header.AckNum,
				"item.key", timeoutItem.Key,
				"type", timeoutPkt.Header.PacketType)
			c.unacked.Remove(timeoutItem.Key)
			c.OnTimeout(timeoutPkt, time.Now())

		case <-idleTimer.C:
			if c.state.stateType != ConnClosed {
				unacked := c.unacked.Keys()
				c.logger.Debug("idle timeout, closing...", "unacked", unacked)
				//c.logger.Warn().Interface("unacked", unacked).Msg("idle timeout expired, closing...")
				c.state.stateType = ConnClosed
				c.state.Err = ErrTimedOut
			}
		case <-c.ctx.Done():
			if !stream.shutdown.Load() {
				c.logger.Debug("ctx done, uTP conn initiating shutdown...", "err", c.ctx.Err())
				stream.shutdown.Store(true)
			}
		}
		//c.logger.Debug("select code block end...")
		if stream.shutdown.Load() && c.state.stateType != ConnClosed {
			c.Shutdown()
		}

		if c.state.stateType == ConnClosed {
			c.logger.Debug("uTP conn closing...", "err", c.state.Err, "c.cid.Send", c.cid.Send, "c.cid.Recv", c.cid.Recv)
			c.ProcessReads()
			c.ProcessWrites(time.Now())

			c.socketEvents <- &SocketEvent{
				Type:         SocketShutdown,
				ConnectionId: c.cid,
			}
			return c.state.Err
		}
	}
}

func (c *Connection) Shutdown() {
	//c.logger.Debug("will shutdown conn", "dst.peer", c.cid.Peer)
	switch c.state.stateType {
	case ConnConnecting:
		c.state.stateType = ConnClosed
	case ConnClosed:
		// ignore
	case ConnConnected:
		if c.state.closing != nil {
			//c.logger.Debug("connection has closingRecord...")
			localFin := c.state.closing.LocalFin
			// If we have not sent our FIN, and there are no pending writes, and there is no
			// pending data in the send buffer, then send our FIN
			if localFin == nil && len(c.pendingWrites) == 0 && c.state.SendBuf.IsEmpty() {
				recvWindow := uint32(c.state.RecvBuf.Available())
				seqNum := c.state.SentPackets.NextSeqNum()
				ackNum := c.state.RecvBuf.AckNum()
				selectiveAck := c.state.RecvBuf.SelectiveAck()

				fin := NewPacketBuilder(
					ST_FIN,
					c.cid.Send,
					uint32(time.Now().UnixMicro()),
					recvWindow,
					seqNum,
				).WithAckNum(ackNum).WithSelectiveAck(selectiveAck).Build()

				c.state.closing.LocalFin = &seqNum
				c.logger.Debug("will send fin packet", "seq", seqNum, "transmitting FIN")
				c.Transmit(fin, time.Now())
			}
		} else {
			var localFin *uint16
			if len(c.pendingWrites) == 0 && c.state.SendBuf.IsEmpty() {
				recvWindow := uint32(c.state.RecvBuf.Available())
				seqNum := c.state.SentPackets.NextSeqNum()
				ackNum := c.state.RecvBuf.AckNum()
				selectiveAck := c.state.RecvBuf.SelectiveAck()

				fin := NewPacketBuilder(
					ST_FIN,
					c.cid.Send,
					uint32(time.Now().UnixMicro()),
					recvWindow,
					seqNum,
				).WithAckNum(ackNum).
					WithSelectiveAck(selectiveAck).
					Build()

				localFin = &seqNum

				c.logger.Debug("transmitting FIN", "seq", seqNum)
				c.Transmit(fin, time.Now())
			}
			c.logger.Debug("set closingRecord", "localFin", localFin)
			c.state.closing = &ClosingRecord{
				LocalFin:  localFin,
				RemoteFin: nil,
			}
		}
	}
}

func (c *Connection) ProcessWrites(now time.Time) {
	var sendBuf *SendBuffer
	var sentPackets *SentPackets
	var recvBuf *ReceiveBuffer
	c.logger.Debug("processWrites start....", "now", now)
	defer c.logger.Debug("processWrites end....", "now", now)

	switch c.state.stateType {
	case ConnConnecting:
		c.logger.Debug("waiting for connection to be established")
		return
	case ConnConnected:
		sendBuf = c.state.SendBuf
		sentPackets = c.state.SentPackets
		recvBuf = c.state.RecvBuf
	case ConnClosed:
		c.logger.Debug("connection is closed, no writes to process")
		result := &ReadOrWriteResult{
			Err: c.state.Err,
		}
		for _, w := range c.pendingWrites {
			w.resultCh <- result
		}
		return
	default:
		return
	}

	// Compose data packets
	nowMicros := time.Now().UnixMicro()
	windowSize := minUint32(sentPackets.Window(), c.peerRecvWindow)
	var payloads [][]byte

	for windowSize > 0 {
		c.logger.Debug("has window size to send a packet data in SendBuffer", "windowSize", windowSize)
		maxDataSize := minUint32(windowSize, uint32(c.config.MaxPacketSize-64))
		data := make([]byte, maxDataSize)
		n := sendBuf.Read(data)
		if n == 0 {
			break
		}
		payloads = append(payloads, data[:n])
		windowSize -= uint32(n)
	}

	// Write pending data to send buffer
	for len(c.pendingWrites) > 0 {
		bufSpace := sendBuf.Available()
		if bufSpace <= 0 {
			break
		}

		writeReq := c.pendingWrites[0]

		if len(writeReq.data) <= bufSpace {
			sendBuf.Write(writeReq.data)
			result := &ReadOrWriteResult{
				Len: len(writeReq.data) + writeReq.written,
			}
			writeReq.resultCh <- result
			c.pendingWrites = c.pendingWrites[1:]
		} else {
			nextWrite := writeReq.data[:bufSpace]
			remainingData := writeReq.data[bufSpace:]
			sendBuf.Write(nextWrite)

			writeReq.data = remainingData
			writeReq.written += bufSpace
		}
		if len(c.writable) == 0 {
			c.writable <- struct{}{}
		}
	}

	// Transmit data packets
	seqNum := sentPackets.NextSeqNum()
	recvWindow := uint32(recvBuf.Available())
	ackNum := recvBuf.AckNum()
	selectiveAck := recvBuf.SelectiveAck()

	for _, payload := range payloads {
		packetInst := NewPacketBuilder(
			ST_DATA,
			c.cid.Send,
			uint32(nowMicros),
			recvWindow,
			seqNum,
		).WithPayload(payload).WithTsDiffMicros(uint32(c.peerTsDiff.Microseconds())).WithAckNum(ackNum).WithSelectiveAck(selectiveAck).Build()

		c.Transmit(packetInst, now)
		seqNum = seqNum + 1 // wrapping add in uint16
	}
}

func (c *Connection) OnWrite(writeReq *QueuedWrite) {
	writeReq.written = 0
	switch c.state.stateType {
	case ConnConnecting:
		// There are 0 bytes written so far
		c.pendingWrites = append(c.pendingWrites, writeReq)

	case ConnConnected:
		if c.state.closing != nil {
			c.logger.Debug("append a queuedWrite to pending writes when closing the conn...")
			if c.state.closing.LocalFin == nil && c.state.closing.RemoteFin != nil {
				c.pendingWrites = append(c.pendingWrites, writeReq)
			} else {
				writeReq.resultCh <- &ReadOrWriteResult{Len: 0}
			}
		} else {
			c.logger.Debug("append a queuedWrite to pending writes")
			c.pendingWrites = append(c.pendingWrites, writeReq)
		}

	case ConnClosed:
		c.logger.Debug("append a queuedWrite to pending writes when closed the conn...")
		result := &ReadOrWriteResult{
			Err: c.state.Err,
			Len: 0,
		}
		writeReq.resultCh <- result
	}

	c.ProcessWrites(time.Now())
	if len(c.writable) == 0 {
		c.writable <- struct{}{}
	}
}

func (c *Connection) ProcessReads() {
	var recvBuf *ReceiveBuffer
	switch c.state.stateType {
	case ConnConnecting:
		return
	case ConnConnected:
		recvBuf = c.state.RecvBuf
	case ConnClosed:
		result := &ReadOrWriteResult{
			Err: c.state.Err,
		}
		c.reads <- result
		c.logger.Debug("read EOF...")
		return
	}

	c.logger.Debug("read data saving in the recvBuf, before...", "avaiable", recvBuf.Available(), "isEmpty", recvBuf.IsEmpty())
	for recvBuf != nil && !recvBuf.IsEmpty() {
		buf := make([]byte, c.config.MaxPacketSize)
		n := recvBuf.Read(buf)
		if n == 0 {
			break
		}
		c.reads <- &ReadOrWriteResult{Data: buf, Len: n}
	}
	c.logger.Debug("read data saving in the recvBuf, after...", "avaiable", recvBuf.Available(), "isEmpty", recvBuf.IsEmpty())

	// If we have reached EOF, send an empty resultCh to all pending reads
	if c.EOF() {
		c.reads <- &ReadOrWriteResult{Data: make([]byte, 0)}
	}
}

func (c *Connection) EOF() bool {
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

func (c *Connection) OnTimeout(packet *Packet, now time.Time) {
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
			c.unacked.Put(seq, packet, timeout)

			// Re-send SYN packet
			synPacket := c.SynPacket(seq)
			select {
			case c.socketEvents <- &SocketEvent{
				Type:         Outgoing,
				Packet:       synPacket,
				ConnectionId: c.cid,
			}:
			default:
			}
		}

	case ConnConnected:
		// If the timed out packet is a SYN, do nothing
		if packet.Header.PacketType == ST_SYN {
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

		// Rebuild and retransmit packet
		recvWindow := uint32(c.state.RecvBuf.Available())
		nowMicros := time.Now().UnixMicro()
		tsDiffMicros := uint32(c.peerTsDiff.Microseconds())

		newPacket := NewPacketBuilder(packet.Header.PacketType, packet.Header.ConnectionId, uint32(nowMicros), recvWindow, packet.Header.SeqNum).
			WithAckNum(c.state.RecvBuf.AckNum()).
			WithSelectiveAck(c.state.RecvBuf.SelectiveAck()).
			WithTsDiffMicros(tsDiffMicros).
			WithPayload(packet.Body).
			Build()

		c.Transmit(newPacket, now)
	}
}

func (c *Connection) OnPacket(packet *Packet, now time.Time) {
	c.logger.Debug("on packet start...",
		"packet.body.len", len(packet.Body),
		"packet.type", packet.Header.PacketType.String(),
		"packet.cid", packet.Header.ConnectionId,
		"packet.seqnum", packet.Header.SeqNum,
		"packet.acknum", packet.Header.AckNum,
		"packet.windowSize", packet.Header.WndSize,
		"now", now)
	defer c.logger.Debug("on packet end...", "now", now)
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
	case ST_SYN:
		c.OnSyn(packet.Header.SeqNum)
	case ST_STATE:
		c.OnState(packet.Header.SeqNum, packet.Header.AckNum)
	case ST_DATA:
		err = c.OnData(packet.Header.SeqNum, packet.Body)
	case ST_FIN:
		err = c.OnFin(packet.Header.SeqNum, packet.Body)
	case ST_RESET:
		c.OnReset()
	}
	if err != nil {
		// todo handle error
	}

	// Process acknowledgments
	switch packet.Header.PacketType {
	case ST_STATE, ST_DATA, ST_FIN:
		delay := time.Duration(packet.Header.TimestampDiff) * time.Microsecond
		if err = c.ProcessAck(packet.Header.AckNum, packet.Eack, delay, now); err != nil {
			c.logger.Warn("ack does not correspond to known seq_num",
				"packet.type", packet.Header.PacketType,
				"packet.seqNum", packet.Header.SeqNum,
				"packet.ackNum", packet.Header.AckNum,
				"err", err)
		}
		c.logger.Debug("process ack end")
	}

	// Handle retransmissions
	c.retransmitLostPackets(now)

	// Send STATE packet if appropriate
	switch packet.Header.PacketType {
	case ST_SYN, ST_DATA, ST_FIN:
		if statePacket := c.StatePacket(); statePacket != nil {
			c.logger.Debug("create a state packet to send out",
				"packet.type", statePacket.Header.PacketType.String(),
				"packet.seqNum", statePacket.Header.SeqNum,
				"packet.ackNum", statePacket.Header.AckNum,
				"packet.cid", statePacket.Header.ConnectionId)
			event := SocketEvent{
				Type:         Outgoing,
				Packet:       statePacket,
				ConnectionId: c.cid,
			}
			select {
			case c.socketEvents <- &event:
			}
		}
	}

	// Notify writable on STATE packets
	if packet.Header.PacketType == ST_STATE {
		c.logger.Debug("put notify writable before...", "now", now, "writableCh.len", len(c.writable))
		//if len(c.writable) == 0 {
		//
		//}
		c.writable <- struct{}{}
		c.logger.Debug("put notify writable end...", "c.writable.len", len(c.writable), "now", now)
	}

	// Notify readable on data or FIN
	if len(packet.Body) > 0 || packet.Header.PacketType == ST_FIN {
		c.readable <- struct{}{}
		c.logger.Debug("notify readable...", "readable.len", len(c.readable))
	}

	// Handle connection closing cases
	if c.state.stateType == ConnConnected && c.state.closing != nil && c.state.closing.LocalFin != nil {
		c.logger.Debug("close connection locally...")
		lastAckNum, isNone := c.state.SentPackets.LastAckNum()
		if !isNone && lastAckNum == *c.state.closing.LocalFin {
			c.state.stateType = ConnClosed
			c.state.Err = nil
		}
	}

	if c.state.stateType == ConnConnected && c.state.closing != nil && c.state.closing.RemoteFin != nil {
		c.logger.Debug("close connection remotely...")
		if !c.state.SentPackets.HasUnackedPackets() && c.state.RecvBuf.AckNum() == *c.state.closing.RemoteFin {
			c.ProcessReads()
			c.state.stateType = ConnClosed
			c.state.Err = nil
		}
	}

	if c.state.stateType == ConnConnected && c.state.closing != nil {
		localFinAck := c.state.closing.LocalFin
		lastAck, isNonce := c.state.SentPackets.LastAckNum()
		if !isNonce && localFinAck != nil && lastAck == *localFinAck {
			c.state.stateType = ConnClosed
			c.state.Err = nil
		}
	}
}

func (c *Connection) ProcessAck(
	ackNum uint16,
	selectiveAck *SelectiveAck,
	delay time.Duration,
	now time.Time,
) error {
	if c.state.stateType != ConnConnected {
		return nil
	}

	fullAcked, selectedAcks, err := c.state.SentPackets.OnAck(
		ackNum, selectiveAck, delay, now)
	if err != nil {
		c.logger.Debug("sent packets OnAck has error", "err", err)
		if errors.Is(err, ErrInvalidAckNum) {
			c.Reset(err)
			return err
		}
		return err
	}
	c.logger.Debug("process ack",
		"fullAcked.start", fullAcked.start,
		"fullAcked.end", fullAcked.end)
	c.unacked.Retain(func(key any) bool {
		return fullAcked.Contains(key.(uint16))
	})
	c.logger.Debug("process ack", "selectedAcks", selectedAcks)
	for _, selectedAck := range selectedAcks {
		c.unacked.Remove(selectedAck)
	}

	return nil
}

func (c *Connection) OnSyn(seqNum uint16) {
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
			c.Reset(err)
		}
	}
}

func (c *Connection) OnState(seqNum, ackNum uint16) {
	if ConnConnecting != c.state.stateType {
		return
	}

	if c.endpoint.Type == Initiator && ackNum == c.endpoint.SynNum {
		// NOTE: In a deviation from the specification, we initialize the ACK num
		// to the sequence number of the SYN-ACK minus 1. This is consistent with
		// the reference implementation and the libtorrent implementation.
		recvBuf := NewReceiveBuffer(c.config.BufferSize, seqNum-1) // wrapping subtraction for uint16
		sendBuf := NewSendBuffer(c.config.BufferSize)

		congestionCtrl := NewDefaultController(FromConnConfig(c.config))
		sentPackets := NewSentPackets(c.endpoint.SynNum, congestionCtrl)

		close(c.state.connectedCh)
		c.state.stateType = ConnConnected
		c.state.RecvBuf = recvBuf
		c.state.SendBuf = sendBuf
		c.state.SentPackets = sentPackets
	}
}

func (c *Connection) OnData(seqNum uint16, data []byte) error {
	c.logger.Debug("on data packet", "seqNum", seqNum, "data.len", len(data))
	// If the data payload is empty, then reset the connection
	if len(data) == 0 {
		c.Reset(ErrEmptyDataPayload)
	}

	switch c.state.stateType {
	case ConnConnecting:
		if c.endpoint.Type == Acceptor {
			return errors.New("unreachable: connection should be marked established")
		} else {
			// connection being established.
			c.logger.Debug("connection being established, ignore", "src.peer", c.cid.Peer, "seqNum", seqNum)
			return nil
		}

	case ConnConnected:
		if c.state.closing != nil && c.state.closing.RemoteFin != nil {
			// Connection is closing
			c.logger.Debug("connection has received remote FIN", "src.peer", c.cid.Peer, "seqNum", seqNum)
			start := c.state.RecvBuf.InitSeqNum()
			seqRange := NewCircularRangeInclusive(start, *c.state.closing.RemoteFin)
			if !seqRange.Contains(seqNum) {
				c.state.stateType = ConnClosed
				c.state.Err = ErrInvalidSeqNum
				return nil
			}
		}
		// not closing should send data
		if len(data) <= c.state.RecvBuf.Available() && !c.state.RecvBuf.WasWritten(seqNum) {
			err := c.state.RecvBuf.Write(data, seqNum)
			if err != nil {
				c.logger.Debug("write data to recv buffer, but available space is not enough",
					"src.peer", c.cid.Peer, "seqNum", seqNum, "data.len", len(data))
			}
			c.logger.Debug("write data to recv buffer, but available space is not enough",
				"src.peer", c.cid.Peer, "seqNum", seqNum, "data.len", len(data), "nextAckNum", c.state.RecvBuf.AckNum())
			return err
		}
	}
	return nil
}

func (c *Connection) OnFin(seqNum uint16, data []byte) error {
	switch c.state.stateType {
	case ConnConnecting, ConnClosed:
		return nil

	case ConnConnected:
		if c.state.closing != nil {
			if c.state.closing.RemoteFin != nil {
				// If we have already received a FIN, a subsequent FIN with a different
				// sequence number is incorrect behavior
				if seqNum != *c.state.closing.RemoteFin {
					c.Reset(ErrInvalidFin)
				}
			} else {
				c.logger.Debug("received FIN", "seq", seqNum)
				remoteFin := seqNum
				c.state.closing.RemoteFin = &remoteFin
				return c.state.RecvBuf.Write(data, seqNum)
			}
		} else {
			// Register the FIN with the receive buffer
			if err := c.state.RecvBuf.Write(data, seqNum); err != nil {
				return err
			}
			c.logger.Debug("received FIN", "seq", seqNum)

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

func (c *Connection) OnReset() {
	c.logger.Warn("RESET from remote")

	// If the connection is not already closed or reset, then reset the connection
	if c.state.stateType != ConnClosed {
		c.Reset(ErrReset)
	}
}

func (c *Connection) Reset(err error) {
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

func (c *Connection) SynPacket(seqNum uint16) *Packet {
	nowMicros := time.Now().UnixMicro()
	return NewPacketBuilder(
		ST_SYN,
		c.cid.Recv,
		uint32(nowMicros),
		c.config.WindowSize,
		seqNum,
	).Build()
}

func (c *Connection) StatePacket() *Packet {
	now := time.Now().UnixMicro()
	tsDiffMicros := uint32(c.peerTsDiff.Microseconds())

	switch c.state.stateType {
	case ConnConnecting:
		if c.endpoint.Type == Initiator {
			return nil
		}

		syn := c.endpoint.SynNum
		synAck := c.endpoint.SynAck
		return NewPacketBuilder(ST_STATE, c.cid.Send, uint32(now), c.config.WindowSize, synAck).
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

		return NewPacketBuilder(ST_STATE, c.cid.Send, uint32(now), recvWindow, seqNum).
			WithTsDiffMicros(tsDiffMicros).
			WithAckNum(ackNum).
			WithSelectiveAck(selectiveAck).
			Build()

	default: // ClosedState
		return nil
	}
}

func (c *Connection) retransmitLostPackets(now time.Time) {
	if !c.state.SentPackets.HasLostPackets() {
		c.logger.Debug("no lost packet")
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
		c.logger.Debug("will retransmit lost packet",
			"packet.type", packetInst.Header.PacketType,
			"packet.seqNum", packetInst.Header.SeqNum,
			"packet.ackNum", packetInst.Header.AckNum,
			"packet.cid", packetInst.Header.ConnectionId,
			"packet.data.len", len(payload))
		c.Transmit(packetInst, now)
	}
}

func (c *Connection) Transmit(packet *Packet, now time.Time) {
	var payload []byte
	var length uint32

	if len(packet.Body) > 0 {
		payload = make([]byte, len(packet.Body))
		copy(payload, packet.Body)
		length = uint32(len(packet.Body))
	}

	c.logger.Debug("will transmit packet",
		"cid", packet.Header.ConnectionId,
		"packet.type", packet.Header.PacketType.String(),
		"packet.seqNum", packet.Header.SeqNum,
		"packet.ackNum", packet.Header.AckNum,
		"unacked.key", packet.Header.SeqNum,
		"packet.body.len", len(packet.Body))

	c.state.SentPackets.OnTransmit(packet.Header.SeqNum, packet.Header.PacketType, payload, length, now)
	c.unacked.Put(packet.Header.SeqNum, packet, c.state.SentPackets.Timeout())

	outbound := SocketEvent{
		Type:         Outgoing,
		Packet:       packet,
		ConnectionId: c.cid,
	}
	c.socketEvents <- &outbound
}
