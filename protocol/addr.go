package protocol

import (
	"fmt"
	"net"
)

type PeerIDs []PeerID

// PeerID uniquely identifies a peer in the network, but it does not provide
// information about the network address of the peer.
type PeerID interface {
	fmt.Stringer

	Equal(PeerID) bool
}

type PeerAddresses []PeerAddress

// PeerAddress stores information about the network address of a peer in the
// network.
type PeerAddress interface {
	fmt.Stringer

	Equal(PeerAddress) bool
	PeerID() PeerID
	NetworkAddress() net.Addr
	IsNewer(PeerAddress) bool
}

type PeerAddressCodec interface {
	Encode(PeerAddress) ([]byte, error)
	Decode([]byte) (PeerAddress, error)
}
