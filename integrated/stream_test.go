package integrated

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	utp "github.com/optimism-java/utp-go"
)

func TestCloseWhenWriteCompletes(t *testing.T) {
	handler := log.NewTerminalHandler(os.Stdout, true)
	log.SetDefault(log.NewLogger(handler))

	connConfig := utp.NewConnectionConfig()

	// Create connected socket pair
	aLink, aCid, bLink, bCid := buildConnectedPair()

	ctx := context.Background()

	// Create sockets
	loggerA := log.Root().New("local", "peerA")
	aSocket := utp.WithSocket(ctx, aLink, loggerA)
	loggerB := log.Root().New("local", "peerB")
	//loggerB := log.NewLogger(log.DiscardHandler())
	bSocket := utp.WithSocket(ctx, bLink, loggerB)

	var wg sync.WaitGroup
	wg.Add(2)

	// Accept connection
	var recvStream *utp.UtpStream
	var recvErr error
	go func() {
		defer wg.Done()
		log.Debug("accepting with cid", "src.peer", bCid.Peer)
		recvStream, recvErr = bSocket.AcceptWithCid(ctx, bCid, connConfig)
		log.Debug("accepted with cid", "src.peer", bCid.Peer)
	}()

	// Connect
	var sendStream *utp.UtpStream
	var sendErr error
	go func() {
		defer wg.Done()
		log.Debug("Connecting with cid", "dst.peer", aCid.Peer)
		sendStream, sendErr = aSocket.ConnectWithCid(ctx, aCid, connConfig)
		if sendErr != nil {
			t.Error("failed to connect with cid", "dst.peer", aCid.Peer)
		}
		log.Debug("Connected", "dst.peer", aCid.Peer)
	}()

	wg.Wait()

	if sendErr != nil {
		t.Fatalf("Connect error: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("Accept error: %v", recvErr)
	}

	// Data to send
	const dataLen = 100
	data := bytes.Repeat([]byte{0xa5}, dataLen)

	// Send data
	var sendWg sync.WaitGroup
	sendWg.Add(1)
	go func() {
		defer sendWg.Done()
		log.Debug("will write to", "dst.peer", sendStream.Cid().Peer)
		written, err := sendStream.Write(ctx, data)
		if err != nil {
			t.Errorf("Error sending data: %v", err)
			return
		}
		if written != dataLen {
			t.Errorf("Expected to write %d bytes, wrote %d", dataLen, written)
		}
	}()

	// Receive data
	var recvWg sync.WaitGroup
	recvWg.Add(1)
	go func() {
		defer recvWg.Done()
		readBuf := make([]byte, 0)
		log.Debug("will read from", "src.peer", recvStream.Cid().Peer)
		_, err := recvStream.ReadToEOF(ctx, &readBuf)
		if err != nil {
			t.Errorf("Error receiving data: %v", err)
			return
		}
		if !bytes.Equal(readBuf, data) {
			t.Error("Received data doesn't match sent data")
		}
	}()

	// Wait for send to complete
	sendWg.Wait()

	// Close stream with timeout
	closeCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()

	done := make(chan struct{})
	go func() {
		sendStream.Close()
		close(done)
	}()

	select {
	case <-closeCtx.Done():
		t.Fatal("Timeout closing stream")
	case <-done:
		// Close successful
	}

	// Wait for receive to complete
	recvWg.Wait()
}
