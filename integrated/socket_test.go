package integrated

import (
	"bytes"
	"context"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	utp "github.com/optimism-java/utp-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	TEST_SOCKET_DATA_LEN = 1_000_000
	TEST_SOCKET_DATA     = bytes.Repeat([]byte{0xa5}, TEST_SOCKET_DATA_LEN)
)

func TestManyConcurrentTransfers(t *testing.T) {
	// CPU 分析
	cpuFile, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	// 内存分析
	memFile, _ := os.Create("mem.prof")
	defer func() {
		pprof.WriteHeapProfile(memFile)
	}()

	// Initialize logging
	logFile, _ := os.OpenFile("test_socket_many_concurrent_transfer.log", os.O_CREATE|os.O_WRONLY, 0644)
	//handler := log.NewTerminalHandler(os.Stdout, true)
	handler := log.NewTerminalHandler(logFile, false)
	log.SetDefault(log.NewLogger(handler))

	t.Log("starting socket test")

	// Setup addresses
	recvAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3400}
	sendAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3401}

	ctx := context.Background()
	// Create sockets
	recvLink, err := utp.Bind(ctx, "udp", recvAddr, log.Root().New("local", ":3400"))
	if err != nil {
		t.Fatalf("Failed to bind recv socket: %v", err)
	}

	sendLink, err := utp.Bind(ctx, "udp", sendAddr, log.Root().New("local", ":3401"))
	if err != nil {
		t.Fatalf("Failed to bind send socket: %v", err)
	}

	// Create wait group for all transfers
	var wg sync.WaitGroup

	const numTransfers = 1_000_000
	start := time.Now()

	// Start transfers
	for i := 0; i < numTransfers; i++ {
		wg.Add(1) // One for sender, one for receiver
		initiateTransfer(
			t, ctx,
			uint16(i*2),
			recvAddr,
			recvLink,
			sendAddr,
			sendLink,
			TEST_SOCKET_DATA,
			&wg)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	go func() {
		// Wait for all transfers in a separate goroutine
		wg.Wait()
		cancel()
	}()
	select {
	case <-timeoutCtx.Done():
	}

	elapsed := time.Since(start)
	megabitsSent := float64(numTransfers) * float64(TEST_SOCKET_DATA_LEN) * 8.0 / 1_000_000.0
	transferRate := megabitsSent / elapsed.Seconds()

	t.Logf("finished high concurrency load test of %d simultaneous transfers, in %v, at a rate of %.0f Mbps",
		numTransfers, elapsed, transferRate)
}

func initiateTransfer(
	t *testing.T,
	ctx context.Context,
	i uint16,
	recvAddr *net.UDPAddr,
	recv *utp.UtpSocket,
	sendAddr *net.UDPAddr,
	send *utp.UtpSocket,
	data []byte,
	wg *sync.WaitGroup) {
	connConfig := utp.NewConnectionConfig()
	initiatorCid := uint16(100) + i
	responderCid := uint16(100) + i + 1

	recvCid := &utp.ConnectionId{
		Send: initiatorCid,
		Recv: responderCid,
		Peer: utp.NewUdpPeer(sendAddr),
	}

	sendCid := &utp.ConnectionId{
		Send: responderCid,
		Recv: initiatorCid,
		Peer: utp.NewUdpPeer(recvAddr),
	}

	// Start receiver goroutine
	go func() {
		defer func() {
			t.Log("wg done")
			wg.Done()
		}()
		stream, err := recv.AcceptWithCid(ctx, recvCid, connConfig)
		if err != nil {
			panic(fmt.Errorf("accept failed: %w", err))
			return
		}

		buf := make([]byte, 0)
		n, err := stream.ReadToEOF(ctx, &buf)
		assert.NoError(t, err, "CID send=%d recv=%d read to eof error: %v",
			recvCid.Send, recvCid.Recv, err)

		t.Logf("CID send=%d recv=%d read %d bytes from uTP stream",
			recvCid.Send, recvCid.Recv, n)
		assert.Equal(t, TEST_SOCKET_DATA_LEN, n,
			"received wrong number of bytes: got %d, want %d", n, len(data))
		assert.True(t, bytes.Equal(data, buf),
			"received data doesn't match sent data")
	}()

	// Start sender goroutine
	go func() {
		defer func() {
			t.Log("wg done")
			wg.Done()
		}()
		stream, err := send.ConnectWithCid(ctx, sendCid, connConfig)
		require.NotNil(t, stream, "stream should not be nil")
		assert.NoError(t, err, "connect failed: %w", err)
		n, err := stream.Write(ctx, data)
		assert.NoError(t, err, "write failed: %w", err)

		assert.Equal(t, TEST_SOCKET_DATA_LEN, n,
			"sent wrong number of bytes: got %d, want %d", n, len(data))
		stream.Close()
	}()
}
