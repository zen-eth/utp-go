package utp_go

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
)

var (
	ErrNotConnected = errors.New("not connected")
)

type UtpStream struct {
	streamCtx    context.Context
	streamCancel context.CancelFunc
	logger       log.Logger
	cid          *ConnectionId
	reads        chan *readOrWriteResult
	writes       chan *queuedWrite
	streamEvents chan *streamEvent
	shutdown     *atomic.Bool
	connHandle   *sync.WaitGroup
	conn         *connection
	closeOnce    sync.Once
	readLocker   sync.Mutex
}

func NewUtpStream(
	ctx context.Context,
	logger log.Logger,
	cid *ConnectionId,
	config *ConnectionConfig,
	syn *packet,
	socketEvents chan *socketEvent,
	streamEvents chan *streamEvent,
	connected chan error,
) *UtpStream {
	logger.Trace("new a utp stream", "dst.peer", cid.Peer, "dst.send", cid.Send, "dst.recv", cid.Recv)
	connHandle := &sync.WaitGroup{}
	connHandle.Add(1)
	streamCtx, cancel := context.WithCancel(ctx)

	utpStream := &UtpStream{
		streamCtx:    streamCtx,
		streamCancel: cancel,
		logger:       logger,
		cid:          cid,
		reads:        make(chan *readOrWriteResult, 100),
		writes:       make(chan *queuedWrite, 100),
		streamEvents: streamEvents,
		connHandle:   connHandle,
		shutdown:     &atomic.Bool{},
	}

	utpStream.conn = newConnection(streamCtx, logger, cid, config, syn, connected, socketEvents, utpStream.reads)
	go utpStream.start()
	return utpStream
}

func (s *UtpStream) Cid() *ConnectionId {
	return s.cid
}

func (s *UtpStream) start() {
	defer s.connHandle.Done()

	err := s.conn.eventLoop(s)
	if err != nil {
		s.logger.Error("utp stream evenLoop has error and return", "err", err)
	}
}

func (s *UtpStream) ReadToEOF(ctx context.Context, buf *[]byte) (int, error) {
	s.readLocker.Lock()
	defer s.readLocker.Unlock()
	n := 0
	var intermediateBuf bytes.Buffer
	for {
		select {
		case <-ctx.Done():
			s.logger.Error("ctx has been canceled", "err", ctx.Err(), "n", n)
			// *buf will not be updated with partial data if context is canceled
			return n, ctx.Err()
		case <-s.streamCtx.Done():
			s.logger.Error("streamCtx has been canceled", "err", s.streamCtx.Err(), "n", n)
			// *buf will not be updated with partial data if stream context is canceled
			return 0, s.streamCtx.Err()
		case res, ok := <-s.reads:
			if !ok { // Channel closed, means EOF
				*buf = intermediateBuf.Bytes()
				return n, nil
			}
			if s.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
				s.logger.Trace("read a new buf", "len", res.Len)
			}
			if res.Err != nil || len(res.Data) == 0 { // Error from channel or explicit EOF signal
				*buf = intermediateBuf.Bytes()
				return n, res.Err
			}
			// Write to intermediate buffer
			written, _ := intermediateBuf.Write(res.Data[:res.Len])
			n += written
			// *buf is updated only at the end or on error/EOF
		}
	}
}

func (s *UtpStream) Write(ctx context.Context, buf []byte) (int, error) {
	if s.shutdown.Load() {
		return 0, ErrNotConnected
	}
	resCh := make(chan *readOrWriteResult, 1)
	s.writes <- &queuedWrite{buf, 0, resCh}
	if s.logger.Enabled(BASE_CONTEXT, log.LevelTrace) {
		s.logger.Trace("created a new queued write to writes channel",
			"dst.peer", s.cid.Peer,
			"buf.len", len(buf),
			"len(s.writes)", len(s.writes),
			"ptr(s)", fmt.Sprintf("%p", s),
			"ptr(writes)", fmt.Sprintf("%p", s.writes))
	}
	var writtenLen int
	var err error
	select {
	case writeRes := <-resCh:
		if writeRes != nil {
			return writeRes.Len, writeRes.Err
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.streamCtx.Done():
		return 0, s.streamCtx.Err()
	}
	return writtenLen, err
}

func (s *UtpStream) Close() {
	s.closeOnce.Do(func() {
		s.logger.Trace("call close utp stream", "dst.Peer", s.cid.Peer, "dst.send", s.cid.Send, "dst.recv", s.cid.Recv)
		s.shutdown.Store(true)
		// wait to consume write buffer and recv buffer
		s.connHandle.Wait()
		s.streamCancel()
	})
}
