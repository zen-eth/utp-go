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

func NewUdpPeer(addr *net.UDPAddr) *UdpPeer {
	return &UdpPeer{addr: addr}
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

func NewConnectionId(peer ConnectionPeer, recvId uint16, sendId uint16) *ConnectionId {
	connId := &ConnectionId{
		Peer: peer,
		Recv: recvId,
		Send: sendId,
	}
	connId.hash = genHash(connId)
	return connId
}

func genHash(connId *ConnectionId) string {
	str := fmt.Sprintf("%d:%d:%v", connId.Send, connId.Recv, connId.Peer.Hash())
	hasher := hasherPool.Get().(hash.Hash)
	defer func() {
		hasher.Reset()
		hasherPool.Put(hasher)
	}()
	hasher.Write([]byte(str))
	bytes := hasher.Sum(nil)[:20]
	return hex.EncodeToString(bytes)
}

func (id *ConnectionId) Hash() string {
	return id.hash
}

func (id *ConnectionId) String() string {
	return fmt.Sprintf("send_id: %d recv_id:%d peer:%v hash: %s", id.Send, id.Recv, id.Peer, id.hash)
}
