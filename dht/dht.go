package dht

import "github.com/renproject/aw/protocol"

type DHT interface {
	PeerAddress(protocol.PeerID) (protocol.PeerAddress, error)
	PeerAddresses() (protocol.PeerAddresses, error)
}
