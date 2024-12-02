package utp_go

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/optimism-java/utp-go/rs"
)

const BUF = 1024 * 1024

var (
	ErrConnected = errors.New("not connected")
)

type UtpStream struct {
	streamCtx    context.Context
	streamCancel context.CancelFunc
	cid          ConnectionId
	reads        chan []byte
	writes       chan []byte
	streamEvent  chan *StreamEvent
	shutdown     bool
	connHandle   *sync.WaitGroup
}

func NewUtpStream(ctx context.Context, cid ConnectionId, config interface{}, syn *rs.Packet, socketEvents chan SocketEvent, streamEvents chan *StreamEvent, connected chan error) *UtpStream {
	reads := make(chan []byte, BUF)
	writes := make(chan []byte, BUF)

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

	go func(utpStream *UtpStream) {
		defer connHandle.Done()
		utpStream.eventLoop()
	}(utpStream)
	return utpStream
}

func (s *UtpStream) Cid() ConnectionId {
	return s.cid
}

func (s *UtpStream) ReadToEOF(ctx context.Context, buf *[]byte) (int, error) {
	n := 0
	for {
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case data, ok := <-s.reads:
			if !ok {
				return n, nil
			}
			if len(data) == 0 {
				// todo log read buffer was sent empty data
				continue
			}
			n += len(data)
			*buf = append(*buf, data...)
		}
	}
}

func (s *UtpStream) Write(ctx context.Context, buf []byte) (int, error) {
	if s.shutdown {
		return 0, ErrConnected
	}
	select {
	case s.writes <- buf:
		return len(buf), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.streamCtx.Done():
		return 0, s.streamCtx.Err()
	}
}

func (s *UtpStream) Close() {
	s.shutdown = true
	s.connHandle.Wait()
	close(s.writes)
	close(s.reads)
	close(s.streamEvent)
	s.streamCancel()
}

func (s *UtpStream) eventLoop() {
	for {
		select {
		case data := <-s.writes:
			fmt.Println("Writing data:", data)
		case event := <-s.streamEvent:
			if event.Type == Incoming {

			} else {
				s.Close()
				return
			}
		case <-s.streamCtx.Done():
			return
		}
	}
}
