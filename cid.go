package utp_go

import "net"

// ConnectionPeer is an interface representing a remote peer.
type ConnectionPeer interface {
	Clone() ConnectionPeer
	Equals(other ConnectionPeer) bool
}

// SocketAddr is a simple implementation of the ConnectionPeer interface using net.UDPAddr.
type SocketAddr struct {
	addr *net.UDPAddr
}

// Clone creates a copy of the SocketAddr.
func (s *SocketAddr) Clone() ConnectionPeer {
	return &SocketAddr{addr: s.addr}
}

// Equals checks if two SocketAddr instances are equal.
func (s *SocketAddr) Equals(other ConnectionPeer) bool {
	if o, ok := other.(*SocketAddr); ok {
		return s.addr.IP.Equal(o.addr.IP) && s.addr.Port == o.addr.Port
	}
	return false
}

// ConnectionId represents a connection identifier with send and receive IDs and a peer.
type ConnectionId struct {
	Send uint16
	Recv uint16
	Peer ConnectionPeer
}
