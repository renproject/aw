package dht

import "github.com/renproject/aw/protocol"

type DHT interface {
	AddPeerAddress(protocol.PeerAddress) error
	RemovePeerAddress(protocol.PeerAddress) error
	PeerAddress(protocol.PeerID) (protocol.PeerAddress, error)
	PeerAddresses() (protocol.PeerAddresses, error)
}
