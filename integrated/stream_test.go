package integrated

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	utp "github.com/optimism-java/utp-go"
)

func TestCloseWhenWriteCompletes(t *testing.T) {
	connConfig := utp.NewConnectionConfig()

	// Create connected socket pair
	sendLink, sendCid, recvLink, recvCid := buildConnectedPair()

	ctx := context.Background()

	// Create sockets
	recvSocket := utp.WithSocket(ctx, recvLink, nil)
	sendSocket := utp.WithSocket(ctx, sendLink, nil)

	var wg sync.WaitGroup
	wg.Add(2)

	// Accept connection
	var recvStream *utp.UtpStream
	var recvErr error
	go func() {
		defer wg.Done()
		log.Debug("accepting with cid", "src.peer", recvCid.Peer)
		recvStream, recvErr = recvSocket.AcceptWithCid(ctx, recvCid, connConfig)
		log.Debug("accepted with cid", "src.peer", recvCid.Peer)
	}()

	// Connect
	var sendStream *utp.UtpStream
	var sendErr error
	go func() {
		defer wg.Done()
		log.Debug("Connecting with cid", "dst.peer", sendCid.Peer)
		sendStream, sendErr = sendSocket.ConnectWithCid(ctx, sendCid, connConfig)
		log.Debug("Connected", "dst.peer", sendCid.Peer)
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
		written, err := recvStream.Write(ctx, data)
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
		_, err := sendStream.ReadToEOF(ctx, &readBuf)
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
	recvWg.Wait()

	// Close stream with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		sendStream.Close()
		close(done)
	}()

	select {
	case <-ctx.Done():
		t.Fatal("Timeout closing stream")
	case <-done:
		// Close successful
	}

	// Wait for receive to complete
	recvWg.Wait()
}
