package protocol

import (
	"fmt"
	"net"
)

type PeerIDs []PeerID

type PeerID interface {
	fmt.Stringer

	Equal(PeerID) bool
}

type PeerAddresses []PeerAddress

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

type PeerIDCodec interface {
	Encode(PeerID) ([]byte, error)
	Decode([]byte) (PeerID, error)
}
