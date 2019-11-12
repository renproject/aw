package protocol

import (
	"net"
)

// PeerID uniquely identifies a peer in the network, but it does not provide
// information about the network address of the peer.
type PeerID interface {
	String() string
	Equal(PeerID) bool
}

// PeerIDs is a list of PeerID
type PeerIDs []PeerID

// PeerAddress stores information about the network address of a peer in the
// network.
type PeerAddress interface {
	String() string
	Equal(PeerAddress) bool
	PeerID() PeerID
	NetworkAddress() net.Addr
	IsNewer(PeerAddress) bool
}

// PeerAddresses is a list of PeerAddress.
type PeerAddresses []PeerAddress

// PeerAddressCodec can encode and decode between PeerAddress and bytes.
type PeerAddressCodec interface {
	Encode(PeerAddress) ([]byte, error)
	Decode([]byte) (PeerAddress, error)
}

// PeerIDCodec can encode and decode between PeerID and bytes.
type PeerIDCodec interface {
	Encode(PeerID) ([]byte, error)
	Decode([]byte) (PeerID, error)
}

// PeerGroupID is the ID of a PeerGroup
type PeerGroupID string
