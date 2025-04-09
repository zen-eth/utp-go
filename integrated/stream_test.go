package integrated

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"
	utp "github.com/zen-eth/utp-go"
)

const expectedIdleTimeout = utp.DefaultMaxIdleTimeout

func TestCloseWhenWriteCompletes(t *testing.T) {
	// Initialize logging
	//logFile, _ := os.OpenFile("test_close_when_write_completes.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	//handler := log.NewTerminalHandlerWithLevel(logFile, log.LevelTrace, false)

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
	const dataLen = 1_000 * 50
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
	closeCtx, cancel := context.WithTimeout(ctx, 160*time.Millisecond)
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

func TestCloseErrorsIfAllPacketsDropped(t *testing.T) {
	handler := log.NewTerminalHandler(os.Stdout, true)
	log.SetDefault(log.NewLogger(handler))
	connConfig := utp.NewConnectionConfig()

	// Create connected socket pair
	aLink, aCid, bLink, bCid := buildConnectedPair()

	// Create sockets
	ctx := context.Background()
	sendSocket := utp.WithSocket(ctx, aLink, log.Root().New("local", "sender"))
	recvSocket := utp.WithSocket(ctx, bLink, log.Root().New("local", "receiver"))

	var wg sync.WaitGroup
	wg.Add(2)

	// Accept connection
	var recvStream *utp.UtpStream
	var recvErr error
	go func() {
		defer wg.Done()
		recvStream, recvErr = recvSocket.AcceptWithCid(ctx, bCid, connConfig)
	}()

	// Connect
	var sendStream *utp.UtpStream
	var sendErr error
	go func() {
		defer wg.Done()
		sendStream, sendErr = sendSocket.ConnectWithCid(ctx, aCid, connConfig)
	}()

	wg.Wait()

	if sendErr != nil {
		t.Fatalf("Connect error: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("Accept error: %v", recvErr)
	}

	// Disable network link, will not receive any packets
	aLink.linkDecider.(*ManualLinkDecider).upSwitch.Store(false)

	// Data to send
	const dataLen = 100
	data := bytes.Repeat([]byte{0xa5}, dataLen)

	// Send data
	var sendWg sync.WaitGroup
	sendWg.Add(1)
	go func() {
		defer sendWg.Done()
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
		_, err := recvStream.ReadToEOF(ctx, &readBuf)
		if err == nil {
			t.Error("Expected timeout error but got none")
			return
		}
		require.ErrorIs(t, err, utp.ErrTimedOut, "Expected timeout error but got: %v", err)

	}()

	// Wait for send to complete
	sendWg.Wait()

	// Try to close stream with timeout
	closeCtx, cancel := context.WithTimeout(ctx, expectedIdleTimeout)
	defer cancel()

	done := make(chan struct{}, 1)
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

func TestCloseSucceedsIfOnlyFinAckDropped(t *testing.T) {
	handler := log.NewTerminalHandler(os.Stdout, true)
	log.SetDefault(log.NewLogger(handler))
	connConfig := utp.NewConnectionConfig()

	// Build connected pair
	sendLink, sendCid, recvLink, recvCid := buildConnectedPair()

	// Create UTP sockets
	ctx := context.Background()
	sendSocket := utp.WithSocket(ctx, sendLink, log.Root().New("local", "sender"))
	recvSocket := utp.WithSocket(ctx, recvLink, log.Root().New("local", "receiver"))

	// Create channels for coordination
	var wg sync.WaitGroup
	wg.Add(2)

	// Accept connection in goroutine
	var recvStream *utp.UtpStream
	var recvErr error
	go func() {
		defer wg.Done()
		recvStream, recvErr = recvSocket.AcceptWithCid(ctx, recvCid, connConfig)
	}()

	// Connect
	var sendStream *utp.UtpStream
	var sendErr error
	go func() {
		defer wg.Done()
		sendStream, sendErr = sendSocket.ConnectWithCid(ctx, sendCid, connConfig)
	}()

	// Wait for connection establishment
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
		written, err := sendStream.Write(ctx, data)
		if err != nil {
			t.Errorf("Error sending data: %v", err)
			return
		}
		if written != dataLen {
			t.Errorf("Wrong number of bytes written. Expected %d, got %d", dataLen, written)
			return
		}
	}()

	// Receive data
	recvComplete := make(chan struct{})
	go func() {
		readBuf := make([]byte, 0)
		_, err := recvStream.ReadToEOF(ctx, &readBuf)
		require.ErrorIs(t, err, utp.ErrTimedOut, "Expected timeout error but got none")
		require.True(t, bytes.Equal(readBuf, data), "Received data doesn't match sent data")
		close(recvComplete)
	}()

	// Wait for send to complete
	sendWg.Wait()

	// Wait for some time to ensure data is fully sent
	time.Sleep(expectedIdleTimeout / 2)

	// Disable network link
	sendLink.linkDecider.(*ManualLinkDecider).upSwitch.Store(false)

	// Try to close send stream with timeout
	sendCloseDone := make(chan struct{})
	go func() {
		sendStream.Close()
		close(sendCloseDone)
	}()

	select {
	case <-sendCloseDone:
	case <-time.After(expectedIdleTimeout * 2):
		t.Error("The send stream timeout on close(), not fast enough")
	}

	// Try to close receive stream
	recvCloseDone := make(chan struct{})
	go func() {
		recvStream.Close()
		close(recvCloseDone)
	}()

	select {
	case <-recvCloseDone:
	case <-time.After(expectedIdleTimeout * 2):
		t.Error("The recv stream did not timeout on close() fast enough")
	}
}

func TestDataValidWhenResendingSynStateResponse(t *testing.T) {
	handler := log.NewTerminalHandler(os.Stdout, true)
	log.SetDefault(log.NewLogger(handler))
	connConfig := utp.NewConnectionConfig()

	connectorSocket, connectorCid, acceptorSocket, acceptorCid := buildLinkDropSentPair(1)

	// Create UTP sockets
	ctx := context.Background()
	acceptor := utp.WithSocket(ctx, acceptorSocket, log.Root().New("local", "acceptor"))
	connector := utp.WithSocket(ctx, connectorSocket, log.Root().New("local", "connector"))

	// Create channels for coordination
	var wg sync.WaitGroup
	wg.Add(2)

	// Accept connection in goroutine
	// Accept connection
	var acceptorStream *utp.UtpStream
	var acceptorErr error
	go func() {
		defer wg.Done()
		acceptorStream, acceptorErr = acceptor.AcceptWithCid(ctx, acceptorCid, connConfig)
	}()

	// Connect
	var connectorStream *utp.UtpStream
	var connectorErr error
	go func() {
		defer wg.Done()
		connectorStream, connectorErr = connector.ConnectWithCid(ctx, connectorCid, connConfig)
	}()

	// Wait for connection establishment
	wg.Wait()
	if acceptorErr != nil {
		t.Fatalf("Accept error: %v", acceptorErr)
	}

	if connectorErr != nil {
		t.Fatalf("Connect error: %v", connectorErr)
	}

	// Data to send
	const dataLen = 9000
	data := bytes.Repeat([]byte{0xa5}, dataLen)

	var sendWg sync.WaitGroup
	sendWg.Add(1)
	go func() {
		defer sendWg.Done()
		written, err := acceptorStream.Write(ctx, data)
		if err != nil {
			t.Errorf("Error sending data: %v", err)
			return
		}
		if written != dataLen {
			t.Errorf("Wrong number of bytes written. Expected %d, got %d", dataLen, written)
			return
		}
	}()

	// Receive data
	recvComplete := make(chan struct{})
	go func() {
		readBuf := make([]byte, 0)
		_, err := connectorStream.ReadToEOF(ctx, &readBuf)
		require.Nil(t, err, "Expected nil but got an error")
		require.True(t, bytes.Equal(readBuf, data), "Received data doesn't match sent data")
		close(recvComplete)
	}()

	// Wait for send to complete
	sendWg.Wait()

	// Try to close send stream with timeout
	acceptorCloseDone := make(chan struct{})
	go func() {
		acceptorStream.Close()
		close(acceptorCloseDone)
	}()

	select {
	case <-acceptorCloseDone:
	case <-time.After(expectedIdleTimeout * 2):
		t.Error("The send stream timeout on close(), not fast enough")
	}

	// Try to close receive stream
	connectorCloseDone := make(chan struct{})
	go func() {
		connectorStream.Close()
		close(connectorCloseDone)
	}()

	select {
	case <-connectorCloseDone:
	case <-time.After(expectedIdleTimeout * 2):
		t.Error("The recv stream did not timeout on close() fast enough")
	}
}
