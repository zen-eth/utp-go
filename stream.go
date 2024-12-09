package utp_go

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

const BUF = 1024 * 1024

var (
	ErrNotConnected = errors.New("not connected")
)

type UtpStream struct {
	streamCtx    context.Context
	streamCancel context.CancelFunc
	cid          *ConnectionId
	reads        chan *ReadOrWriteResult
	writes       chan *QueuedWrite
	streamEvents chan *StreamEvent
	shutdown     bool
	shutdownCh   chan struct{}
	connHandle   *sync.WaitGroup
	conn         *Connection
}

func NewUtpStream(
	ctx context.Context,
	cid *ConnectionId,
	config *ConnectionConfig,
	syn *Packet,
	socketEvents chan *SocketEvent,
	streamEvents chan *StreamEvent,
	connected chan error,
) *UtpStream {
	log.Debug("new a utp stream", "dst.peer", cid.Peer, "dst.send", cid.Send, "dst.recv", cid.Recv)
	connHandle := &sync.WaitGroup{}
	connHandle.Add(1)
	streamCtx, cancel := context.WithCancel(ctx)

	utpStream := &UtpStream{
		streamCtx:    streamCtx,
		streamCancel: cancel,
		cid:          cid,
		reads:        make(chan *ReadOrWriteResult, 10),
		writes:       make(chan *QueuedWrite, 1),
		streamEvents: streamEvents,
		connHandle:   connHandle,
		shutdownCh:   make(chan struct{}, 1),
	}

	utpStream.conn = NewConnection(streamCtx, cid, config, syn, connected, socketEvents, utpStream.reads)

	go utpStream.start()
	return utpStream
}

func (s *UtpStream) Cid() *ConnectionId {
	return s.cid
}

func (s *UtpStream) start() {
	defer s.connHandle.Done()

	err := s.conn.EventLoop(s)
	if err != nil {
		log.Error("utp stream evenLoop has error and return", "err", err)
	}
}

func (s *UtpStream) ReadToEOF(ctx context.Context, buf *[]byte) (int, error) {
	n := 0
	data := make([]byte, 0)
	for {
		select {
		case <-ctx.Done():
			log.Error("ctx has been canceled", "err", ctx.Err(), "n", n)
			return n, ctx.Err()
		case <-s.streamCtx.Done():
			log.Error("streamCtx has been canceled", "err", s.streamCtx.Err(), "n", n)
			return 0, s.streamCtx.Err()
		case res, ok := <-s.reads:
			if !ok {
				return n, nil
			}
			log.Debug("read a new buf", "len", len(res.Data))
			if len(res.Data) == 0 {
				return n, nil
			}
			n += len(res.Data)
			data = append(data, res.Data...)
			buf = &data
		}
	}
}

func (s *UtpStream) Write(ctx context.Context, buf []byte) (int, error) {
	if s.shutdown {
		return 0, ErrNotConnected
	}
	resCh := make(chan *ReadOrWriteResult, 1)
	select {
	case s.writes <- &QueuedWrite{buf, 0, resCh}:
		log.Debug("created a new queued write to writes channel",
			"dst.peer", s.cid.Peer,
			"buf.len", len(buf),
			"len(s.writes)", len(s.writes),
			"ptr(s)", fmt.Sprintf("%p", s),
			"ptr(writes)", fmt.Sprintf("%p", s.writes))
	}

	var err error
	var writtenLen int
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
	s.shutdown = true
	s.streamCancel()
	s.connHandle.Wait()
	close(s.writes)
	close(s.reads)
	close(s.streamEvents)

}
