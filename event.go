package utp_go

type StreamEventType int

const (
	streamIncoming StreamEventType = iota
	streamShutdown
)

type socketEventType int

const (
	outgoing socketEventType = iota
	socketShutdown
)

type streamEvent struct {
	Type   StreamEventType
	Packet *Packet
}

// socketEvent represents events related to a socket.
type socketEvent struct {
	Type         socketEventType
	Packet       *Packet
	ConnectionId ConnectionPeer
}
