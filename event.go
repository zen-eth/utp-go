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
	Packet *packet
}

// socketEvent represents events related to a socket.
type socketEvent struct {
	Type         socketEventType
	Packet       *packet
	ConnectionId ConnectionPeer
}

func newOutgoingSocketEvent(p *packet, cid ConnectionPeer) *socketEvent {
	return &socketEvent{
		Type:         outgoing,
		Packet:       p,
		ConnectionId: cid,
	}
}

func newShutdownSocketEvent(cid ConnectionPeer) *socketEvent {
	return &socketEvent{
		Type:         socketShutdown,
		ConnectionId: cid,
	}
}
