package utp_go

import (
	"context"
	"errors"
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
	streamEvent  chan StreamEvent
	shutdown     bool
	shutdownCh   chan struct{}
	connHandle   *sync.WaitGroup
}

func NewUtpStream(
	ctx context.Context,
	cid *ConnectionId,
	config *ConnectionConfig,
	syn *Packet,
	socketEvents chan<- SocketEvent,
	streamEvents chan StreamEvent,
	connected chan error,
) *UtpStream {
	reads := make(chan *ReadOrWriteResult, 10)
	writes := make(chan *QueuedWrite, 10)
	shutdownCh := make(chan struct{}, 1)

	connHandle := &sync.WaitGroup{}
	connHandle.Add(1)
	streamCtx, cancel := context.WithCancel(ctx)

	utpStream := &UtpStream{
		streamCtx:    streamCtx,
		streamCancel: cancel,
		cid:          cid,
		reads:        reads,
		writes:       writes,
		streamEvent:  streamEvents,
		connHandle:   connHandle,
	}

	conn := NewConnection(streamCtx, cid, config, syn, connected, socketEvents, reads)

	go func(utpStream *UtpStream) {
		defer connHandle.Done()
		err := conn.EventLoop(streamEvents, writes, shutdownCh)
		if err != nil {
			log.Error("utp stream evenLoop has error and return", "err", err)
		}

	}(utpStream)
	return utpStream
}

func (s *UtpStream) Cid() *ConnectionId {
	return s.cid
}

func (s *UtpStream) ReadToEOF(ctx context.Context, buf *[]byte) (int, error) {
	n := 0
	data := make([]byte, 0)
	for {
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case <-s.streamCtx.Done():
			return 0, s.streamCtx.Err()
		case res, ok := <-s.reads:
			if !ok {
				return n, nil
			}
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
	s.writes <- &QueuedWrite{buf, 0, resCh}

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
	close(s.streamEvent)

}
