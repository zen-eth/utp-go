package utp_go

import (
	"bytes" // Required for bytes.Buffer if ReadToEOF uses it, or for test data generation
	"context"
	"testing"
	// "time" // Not strictly needed for this benchmark's logic if not using timeouts within ReadToEOF

	"github.com/ethereum/go-ethereum/log"
)

func BenchmarkUtpStreamReadToEOF_ManySmallChunks(b *testing.B) {
	const (
		numChunks  = 1000
		chunkSize  = 50
		bufferHint = numChunks * chunkSize // Used for pre-allocating the output buffer
	)

	// Basic setup for UtpStream - these can be further simplified if they don't affect ReadToEOF path
	config := NewConnectionConfig() // Use default config
	cid := &ConnectionId{
		Send: 123,
		Recv: 456,
		Peer: nil, // Assuming Peer is not directly used by ReadToEOF path being benchmarked
	}
	logger := log.NewNopLogger() // Suppress logging output during benchmark

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer() // Stop timer for the setup phase of each iteration

		// Create a new UtpStream for each benchmark iteration to ensure isolation
		// The stream's internal context for its event loop
		streamCtx, streamCancel := context.WithCancel(context.Background())

		// Mock channels that NewUtpStream might need.
		// For ReadToEOF, the primary dependency is s.reads.
		// Other channels (socketEvents, streamEvents) are less critical if eventLoop doesn't interfere.
		socketEvents := make(chan *socketEvent) // Unbuffered, likely not used by ReadToEOF directly
		streamEventsChan := make(chan *streamEvent) // Renamed to avoid conflict with streamEvents in UtpStream struct
		connected := make(chan error, 1)        // Buffered to prevent NewUtpStream from blocking

		// The s.reads channel is crucial and will be directly controlled by the benchmark.
		// This channel will be populated with data to be read by ReadToEOF.
		currentReadsChan := make(chan *readOrWriteResult, numChunks)

		// Create the stream instance.
		// The goal is to have `s.reads` be `currentReadsChan`.
		// NewUtpStream internally creates a connection which sets up its own reads channel.
		// We will replace s.reads with our own channel after the stream is created.
		s := NewUtpStream(streamCtx, logger, cid, config, nil, socketEvents, streamEventsChan, connected)
		s.reads = currentReadsChan // Override the reads channel

		// Populate the reads channel with many small data chunks
		for j := 0; j < numChunks; j++ {
			data := make([]byte, chunkSize)
			// Simple data pattern, content doesn't matter much for the benchmark focus
			for k := 0; k < chunkSize; k++ {
				data[k] = byte((j + k) % 256)
			}
			currentReadsChan <- &readOrWriteResult{Data: data, Len: chunkSize, Err: nil}
		}
		close(currentReadsChan) // Close the channel to signal EOF to ReadToEOF

		// outputBuf will store the data read by ReadToEOF
		var outputBuf []byte // Benchmark will measure allocations for this
		// For a more controlled benchmark of just the append logic, preallocate:
		// outputBuf := make([]byte, 0, bufferHint)

		b.StartTimer() // Start timer right before calling ReadToEOF

		_, err := s.ReadToEOF(context.Background(), &outputBuf) // Use a fresh context for the call itself

		b.StopTimer() // Stop timer immediately after ReadToEOF returns

		// Basic validation (optional, but good for sanity checking)
		if err != nil {
			b.Fatalf("ReadToEOF returned an error: %v", err)
		}
		expectedLen := numChunks * chunkSize
		if len(outputBuf) != expectedLen {
			b.Fatalf("Expected output buffer length %d, got %d", expectedLen, len(outputBuf))
		}

		// Cleanup for this iteration:
		// Cancel the stream's context to signal its internal eventLoop to stop.
		streamCancel()
		// Wait for the connection's event loop to actually terminate.
		// This is important to prevent goroutine leakage and interference between benchmark iterations.
		s.connHandle.Wait()
	}
}
