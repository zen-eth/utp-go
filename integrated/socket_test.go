package integrated

import (
	"bytes"
	"context"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"runtime/trace"
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
	logFile, _ := os.OpenFile("test_socket_many_concurrent_transfer.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
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
	defer recvLink.Close()

	sendLink, err := utp.Bind(ctx, "udp", sendAddr, log.Root().New("local", ":3401"))
	if err != nil {
		t.Fatalf("Failed to bind send socket: %v", err)
	}
	defer sendLink.Close()
	// Create wait group for all transfers
	var wg sync.WaitGroup

	const numTransfers = 100
	start := time.Now()

	// Start transfers
	for i := 0; i < numTransfers; i++ {
		wg.Add(2) // One for sender, one for receiver
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

func TestUdpTransfer(t *testing.T) {
	recvAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3600}
	recvConn, err := net.ListenUDP("udp", recvAddr)
	require.NoError(t, err, "Failed to bind recv socket")
	sendAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3601}
	sendConn, err := net.ListenUDP("udp", sendAddr)

	var wg sync.WaitGroup
	hugeData := bytes.Repeat([]byte{0xa5}, 50*1024*1024)
	length := 1024 * 1024 * 50
	start := time.Now()
	wg.Add(2)
	go func() {
		defer wg.Done()
		recvBuf := make([]byte, 50*1024*1024)
		startIndex := 0
		for startIndex < length {
			n, addr, err := recvConn.ReadFrom(recvBuf[startIndex:])
			require.Equal(t, sendAddr.String(), addr.String(), "addr mismatch")
			require.NoError(t, err, "Failed to recv data")
			startIndex += n
		}
	}()

	go func() {
		defer wg.Done()
		offset := 0
		const onceSize = 980
		for offset < length {
			startIndex := offset
			endIndex := offset + onceSize
			if endIndex > length {
				endIndex = length
			}
			n, err := sendConn.WriteToUDP(hugeData[startIndex:endIndex], recvAddr)
			require.NoError(t, err, "Failed to send data")
			require.Equal(t, endIndex-startIndex, n, "Failed to send all data")
			offset = endIndex
		}

	}()
	wg.Wait()

	elapsed := time.Since(start)
	megabytesSent := float64(length) / 1_000_000.0
	megabitsSent := megabytesSent * 8.0
	transferRate := megabitsSent / elapsed.Seconds()

	t.Logf(
		"finished single large transfer test with %.0f MB, in %v, at a rate of %.1f Mbps",
		megabytesSent,
		elapsed,
		transferRate,
	)
}

func TestOneHugeDataTransfer(t *testing.T) {
	// CPU 分析
	cpuFile, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	// trace 分析
	traceFile, _ := os.Create("trace.prof")
	trace.Start(traceFile)
	defer trace.Stop()
	// 内存分析
	//memFile, _ := os.Create("mem.prof")
	//defer func() {
	//	pprof.WriteHeapProfile(memFile)
	//}()
	//logFile, _ := os.OpenFile("test_one_huge_data_transfer.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	//handler := log.NewTerminalHandler(logFile, false)
	//handler := log.NewTerminalHandler(os.Stdout, true)
	//log.SetDefault(log.NewLogger(handler))

	// 创建50MB的测试数据
	hugeData := bytes.Repeat([]byte{0xf0}, 1024*1024*50)

	t.Log("starting single transfer of huge data test")

	recvAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3500}
	sendAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3501}

	ctx := context.Background()
	recv, err := utp.Bind(ctx, "udp", recvAddr, log.Root().New("local", ":3500"))
	require.NoError(t, err, "Failed to bind recv socket")
	send, err := utp.Bind(ctx, "udp", sendAddr, log.Root().New("local", ":3501"))

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(2)

	// 启动传输
	go func() {
		initiateTransfer(t, ctx, uint16(2), recvAddr, recv, sendAddr, send, hugeData, &wg)
	}()

	// 等待接收完成
	timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Second)
	go func() {
		wg.Wait()
		cancel()
	}()

	select {
	case <-timeoutCtx.Done():
		t.Fatalf("huge data transfer timed out")
	}

	elapsed := time.Since(start)
	megabytesSent := float64(len(hugeData)) / 1_000_000.0
	megabitsSent := megabytesSent * 8.0
	transferRate := megabitsSent / elapsed.Seconds()

	t.Logf(
		"finished single large transfer test with %.0f MB, in %v, at a rate of %.1f Mbps",
		megabytesSent,
		elapsed,
		transferRate,
	)
}

func TestEmptySocketConnCount(t *testing.T) {
	socketAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3402}

	// 假设我们有一个 UtpSocket 的包装结构
	utpSocket, err := utp.Bind(context.Background(), "udp", socketAddr, nil)
	require.NoError(t, err, "Failed to bind socket")
	defer utpSocket.Close()

	require.Equal(t, 0, utpSocket.NumConnections(), "Expected 0 connections, got %d", utpSocket.NumConnections())
}

func TestSocketReportsTwoConns(t *testing.T) {
	ctx := context.Background()
	connConfig := utp.NewConnectionConfig()
	// create receive socket
	recvAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3404}
	recv, err := utp.Bind(ctx, "udp", recvAddr, nil)
	require.NoError(t, err, "Failed to bind recv socket")

	// 创建发送socket
	sendAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3405}
	send, err := utp.Bind(ctx, "udp", sendAddr, nil)
	require.NoError(t, err, "Failed to bind send socket")

	// 定义连接ID
	recvOneCid := &utp.ConnectionId{
		Send: 100,
		Recv: 101,
		Peer: utp.NewUdpPeer(sendAddr),
	}
	sendOneCid := &utp.ConnectionId{
		Send: 101,
		Recv: 100,
		Peer: utp.NewUdpPeer(recvAddr),
	}

	recvTwoCid := &utp.ConnectionId{
		Send: 200,
		Recv: 201,
		Peer: utp.NewUdpPeer(sendAddr),
	}
	sendTwoCid := &utp.ConnectionId{
		Send: 201,
		Recv: 200,
		Peer: utp.NewUdpPeer(recvAddr),
	}

	var wg sync.WaitGroup
	wg.Add(4)

	// 启动第一对连接
	go func() {
		defer wg.Done()
		recv.AcceptWithCid(ctx, recvOneCid, connConfig)
	}()

	go func() {
		defer wg.Done()
		send.ConnectWithCid(ctx, sendOneCid, connConfig)
	}()

	// 启动第二对连接
	go func() {
		defer wg.Done()
		recv.AcceptWithCid(ctx, recvTwoCid, connConfig)
	}()

	go func() {
		defer wg.Done()
		send.ConnectWithCid(ctx, sendTwoCid, connConfig)
	}()

	wg.Wait()

	if recv.NumConnections() != 2 {
		t.Errorf("Expected 2 connections for receiver, got %d", recv.NumConnections())
	}
	if send.NumConnections() != 2 {
		t.Errorf("Expected 2 connections for sender, got %d", send.NumConnections())
	}
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
			t.Log("writer wg done")
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
		assert.Equal(t, len(data), n,
			"received wrong number of bytes: got %d, want %d", n, len(data))
		assert.True(t, bytes.Equal(data, buf),
			"received data doesn't match sent data")
	}()

	// Start sender goroutine
	go func() {
		defer func() {
			t.Log("writer wg done")
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
