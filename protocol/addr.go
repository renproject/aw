package protocol

import (
	"fmt"
	"net"
)

type PeerID interface {
	fmt.Stringer
}

type PeerAddress interface {
	fmt.Stringer

	PeerID() PeerID
	NetworkAddress() net.Addr
}

type PeerAddressCodec interface {
	Encode(PeerAddress) ([]byte, error)
	Decode([]byte) (PeerAddress, error)
}
