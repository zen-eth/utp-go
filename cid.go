package utp_go

import (
	"encoding/hex"
	"fmt"
	"hash"
	"net"
	"sync"

	"golang.org/x/crypto/sha3"
)

var hasherPool = sync.Pool{
	New: func() interface{} {
		return sha3.New256()
	},
}

// ConnectionPeer is an interface representing a remote peer.
type ConnectionPeer interface {
	Hash() string
}

// SocketAddr is a simple implementation of the ConnectionPeer interface using net.UDPAddr.
type UdpPeer struct {
	addr *net.UDPAddr
}

func (p *UdpPeer) Hash() string {
	return p.addr.String()
}

func (p *UdpPeer) String() string {
	return p.addr.String()
}

type TcpPeer struct {
	addr *net.TCPAddr
}

func (p *TcpPeer) Hash() string {
	return p.addr.String()
}

func (p *TcpPeer) String() string {
	return p.addr.String()
}

// ConnectionId represents a connection identifier with send and receive IDs and a peer.
type ConnectionId struct {
	Send uint16
	Recv uint16
	Peer ConnectionPeer
	hash string
}

func (id *ConnectionId) Hash() string {
	if id.hash == "" {
		str := fmt.Sprintf("%d:%d:%v", id.Send, id.Recv, id.Peer.Hash())
		hasher := hasherPool.Get().(hash.Hash)
		defer func() {
			hasher.Reset()
			hasherPool.Put(hasher)
		}()
		hasher.Write([]byte(str))
		bytes := hasher.Sum(nil)[:20]
		id.hash = hex.EncodeToString(bytes)
	}
	return id.hash
}

func (id *ConnectionId) String() string {
	return fmt.Sprintf("send_id: %d recv_id:%d peer:%v", id.Send, id.Recv, id.Peer)
}
