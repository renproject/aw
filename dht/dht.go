package dht

import (
	"fmt"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
)

type DHT interface {
	Me() protocol.PeerAddress
	AddPeerAddress(protocol.PeerAddress) error
	AddPeerAddresses(peerAddrs protocol.PeerAddresses) error
	RemovePeerAddress(protocol.PeerID) error
	PeerAddress(protocol.PeerID) (protocol.PeerAddress, error)
	PeerAddresses() (protocol.PeerAddresses, error)
	PeerCount() (int, error)
}

type dht struct {
	me    protocol.PeerAddress
	codec protocol.PeerAddressCodec
	store kv.Iterable
}

func New(me protocol.PeerAddress, codec protocol.PeerAddressCodec, store kv.Iterable) DHT {
	return &dht{
		me:    me,
		codec: codec,
		store: store,
	}
}

func (dht *dht) Me() protocol.PeerAddress {
	return dht.me
}

func (dht *dht) PeerCount() (int, error) {
	return dht.store.Size()
}

func (dht *dht) PeerAddresses() (protocol.PeerAddresses, error) {
	peerAddrs := []protocol.PeerAddress{}
	iterator, err := dht.store.Iterator()
	if err != nil {
		return nil, err
	}

	for iterator.Next() {
		peer := []byte{}
		if err := iterator.Value(&peer); err != nil {
			return nil, err
		}
		peerAddr, err := dht.codec.Decode(peer)
		if err != nil {
			return nil, err
		}
		peerAddrs = append(peerAddrs, peerAddr)
	}

	return peerAddrs, nil
}

func (dht *dht) AddPeerAddress(peerAddr protocol.PeerAddress) error {
	data, err := dht.codec.Encode(peerAddr)
	if err != nil {
		return fmt.Errorf("failed to stringify the given multi address: %v", err)
	}
	return dht.store.Insert(peerAddr.PeerID().String(), data)
}

func (dht *dht) AddPeerAddresses(peerAddrs protocol.PeerAddresses) error {
	for _, addr := range peerAddrs {
		if err := dht.AddPeerAddress(addr); err != nil {
			return err
		}
	}
	return nil
}

func (dht *dht) PeerAddress(id protocol.PeerID) (protocol.PeerAddress, error) {
	peer := []byte{}
	if err := dht.store.Get(id.String(), peer); err != nil {
		return nil, err
	}
	return dht.codec.Decode(peer)
}

func (dht *dht) RemovePeerAddress(id protocol.PeerID) error {
	return dht.store.Delete(id.String())
}
