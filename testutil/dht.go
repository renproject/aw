package testutil

import (
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
)

// NewDHT creates a new DHT with given PeerAddress,
func NewDHT(address protocol.PeerAddress) dht.DHT{
	codec := NewSimpleTCPPeerAddressCodec()
	db := kv.NewMemDB(kv.JSONCodec)
	table := kv.NewTable(db, "dht")
	dht, err := dht.New(address, codec, table)
	if err != nil {
		panic(err)
	}
	return dht
}