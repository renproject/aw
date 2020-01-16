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

// GroupID uniquely identifies a group of PeerIDs.
type GroupID [32]byte

// NilGroupID is a reserved GroupID and usually represents all known PeerIDs.
var NilGroupID = GroupID{}

// Equal compares two groupIDs and return if they are same.
func (id GroupID) Equal(another GroupID) bool {
	return bytes.Equal(id[:], another[:])
}

// ValidateGroupID checks if the GroupID is valid under the given message
// variant
func ValidateGroupID(groupID GroupID, variant MessageVariant) error {
	if variant != Broadcast && variant != Multicast {
		if groupID != NilGroupID {
			return ErrInvalidGroupID
		}
	}
	return nil
}
