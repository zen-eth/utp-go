package utp_go

import "github.com/optimism-java/utp-go/rs"

type StreamEventType int

const (
	Incoming StreamEventType = iota
	Shutdown
)

type SocketEventType int

const (
	Outgoing SocketEventType = iota
	SocketShutdown
)

type StreamEvent struct {
	Type   StreamEventType
	Packet *rs.Packet
}

// SocketEvent represents events related to a socket.
type SocketEvent struct {
	Type         SocketEventType
	Packet       *rs.Packet
	ConnectionId *ConnectionId
}
