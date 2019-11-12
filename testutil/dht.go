package testutil

import (
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
)

// NewDHT creates a new DHT with given PeerAddress,
func NewDHT(address protocol.PeerAddress, store kv.Table, bootstrapAddresses protocol.PeerAddresses) dht.DHT {
	codec := NewSimpleTCPPeerAddressCodec()
	dht, err := dht.New(address, codec, store, bootstrapAddresses...)
	if err != nil {
		panic(err)
	}
	return dht
}

func NewTable(name string) kv.Table {
	db := kv.NewMemDB(kv.JSONCodec)
	return kv.NewTable(db, name)
}
