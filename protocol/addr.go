package protocol

import (
	"bytes"
	"net"
)

// PeerID uniquely identifies a peer in the network, but it does not provide
// information about the network address of the peer.
type PeerID interface {
	String() string
	Equal(PeerID) bool
}

// PeerIDs is a list of PeerID.
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

// PeerGroup uniquely identifies a group of PeerIDs.
type PeerGroupID [32]byte

// NilPeerGroupID is a reserved GroupID and usually represents all known PeerIDs.
var NilPeerGroupID = PeerGroupID{}

// Equal compares two groupIDs and return if they are same.
func (id PeerGroupID) Equal(another PeerGroupID) bool {
	return bytes.Equal(id[:], another[:])
}

// ValidatePeerGroupID checks if the PeerGroupID is valid under the given message
// variant
func ValidatePeerGroupID(groupID PeerGroupID, variant MessageVariant) error {
	if variant != Broadcast && variant != Multicast {
		if groupID != NilPeerGroupID {
			return ErrInvalidPeerGroupID
		}
	}
	return nil
}
