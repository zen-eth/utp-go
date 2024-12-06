package utp_go

type StreamEventType int

const (
	StreamIncoming StreamEventType = iota
	StreamShutdown
)

type SocketEventType int

const (
	Outgoing SocketEventType = iota
	SocketShutdown
)

type StreamEvent struct {
	Type   StreamEventType
	Packet *Packet
}

// SocketEvent represents events related to a socket.
type SocketEvent struct {
	Type         SocketEventType
	Packet       *Packet
	ConnectionId ConnectionPeer
}
