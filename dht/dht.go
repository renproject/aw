package dht

import (
	"fmt"
	"sync"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
)

// A DHT is a distributed hash table. It is used for storing peer addresses. A
// DHT is not required to be persistent and will often purge stale peer
// addresses.
type DHT interface {
	Me() protocol.PeerAddress
	NumPeers() (int, error)
	PeerAddress(protocol.PeerID) (protocol.PeerAddress, error)
	PeerAddresses() (protocol.PeerAddresses, error)

	AddPeerAddress(protocol.PeerAddress) error
	AddPeerAddresses(protocol.PeerAddresses) error
	RemovePeerAddress(protocol.PeerID) error
}

type dht struct {
	me    protocol.PeerAddress
	codec protocol.PeerAddressCodec
	store kv.Iterable

	inMemCacheMu *sync.RWMutex
	inMemCache   map[string]protocol.PeerAddress
}

// New DHT that stores peer addresses in the given store. It will cache all peer
// addresses in memory for fast access. It is safe for concurrent use,
// regardless of the underlying store.
func New(me protocol.PeerAddress, codec protocol.PeerAddressCodec, store kv.Iterable, bootstrapAddrs ...protocol.PeerAddress) (DHT, error) {
	dht := &dht{
		me:    me,
		codec: codec,
		store: store,

		inMemCacheMu: new(sync.RWMutex),
		inMemCache:   map[string]protocol.PeerAddress{},
	}

	if err := dht.fillInMemCache(); err != nil {
		return nil, err
	}

	if count, err := dht.NumPeers(); count == 0 || err != nil {
		for _, addr := range bootstrapAddrs {
			if err := dht.addPeerAddressWithoutLock(addr); err != nil {
				return nil, fmt.Errorf("failed to store bootstrap addresses: %v", err)
			}
		}
	}
	return dht, nil
}

func (dht *dht) Me() protocol.PeerAddress {
	return dht.me
}

func (dht *dht) NumPeers() (int, error) {
	dht.inMemCacheMu.RLock()
	defer dht.inMemCacheMu.RUnlock()

	return len(dht.inMemCache), nil
}

func (dht *dht) PeerAddresses() (protocol.PeerAddresses, error) {
	dht.inMemCacheMu.RLock()
	defer dht.inMemCacheMu.RUnlock()

	peerAddrs := make(protocol.PeerAddresses, 0, len(dht.inMemCache))
	for _, peerAddr := range dht.inMemCache {
		peerAddrs = append(peerAddrs, peerAddr)
	}

	return peerAddrs, nil
}

func (dht *dht) AddPeerAddress(peerAddr protocol.PeerAddress) error {
	dht.inMemCacheMu.Lock()
	defer dht.inMemCacheMu.Unlock()

	return dht.addPeerAddressWithoutLock(peerAddr)
}

func (dht *dht) AddPeerAddresses(peerAddrs protocol.PeerAddresses) error {
	dht.inMemCacheMu.Lock()
	defer dht.inMemCacheMu.Unlock()

	for _, peerAddr := range peerAddrs {
		if err := dht.addPeerAddressWithoutLock(peerAddr); err != nil {
			return err
		}
	}
	return nil
}

func (dht *dht) PeerAddress(id protocol.PeerID) (protocol.PeerAddress, error) {
	dht.inMemCacheMu.RLock()
	defer dht.inMemCacheMu.RUnlock()

	peerAddr, ok := dht.inMemCache[id.String()]
	if !ok {
		return nil, fmt.Errorf("peer=%v not found", id)
	}
	return peerAddr, nil
}

func (dht *dht) RemovePeerAddress(id protocol.PeerID) error {
	dht.inMemCacheMu.Lock()
	defer dht.inMemCacheMu.Unlock()

	if err := dht.store.Delete(id.String()); err != nil {
		return fmt.Errorf("error deleting peer=%v from dht: %v", id, err)
	}
	delete(dht.inMemCache, id.String())

	return nil
}

func (dht *dht) addPeerAddressWithoutLock(peerAddr protocol.PeerAddress) error {
	data, err := dht.codec.Encode(peerAddr)
	if err != nil {
		return fmt.Errorf("error encoding peer address=%v: %v", peerAddr, err)
	}
	if err := dht.store.Insert(peerAddr.PeerID().String(), data); err != nil {
		return fmt.Errorf("error inserting peer address=%v into dht: %v", peerAddr, err)
	}
	dht.inMemCache[peerAddr.PeerID().String()] = peerAddr
	return nil
}

func (dht *dht) fillInMemCache() error {
	iter, err := dht.store.Iterator()
	if err != nil {
		return fmt.Errorf("error initialising dht iterator: %v", err)
	}
	for iter.Next() {
		var peerAddr protocol.PeerAddress
		if err := iter.Value(&peerAddr); err != nil {
			return fmt.Errorf("error scanning dht iterator: %v", err)
		}
		dht.inMemCache[peerAddr.PeerID().String()] = peerAddr
	}
	return nil
}
