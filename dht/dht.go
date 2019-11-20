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

	// Me returns self PeerAddress
	Me() protocol.PeerAddress

	// NumPeers returns total number of PeerAddresses stored in the DHT.
	NumPeers() (int, error)

	// PeerAddress returns the resolved protocol.PeerAddress of the given
	// PeerID. It returns an ErrPeerNotFound if the PeerID cannot be found.
	PeerAddress(protocol.PeerID) (protocol.PeerAddress, error)

	// PeerAddresses returns all the PeerAddresses stored in the DHT.
	PeerAddresses() (protocol.PeerAddresses, error)

	// AddPeerAddress adds a PeerAddress into the DHT.
	AddPeerAddress(protocol.PeerAddress) error

	// UpdatePeerAddress tries to update the PeerAddress in the DHT. It returns
	// true if the given peerAddr is newer than the one we stored.
	UpdatePeerAddress(protocol.PeerAddress) (bool, error)

	// RemovePeerAddress removes the PeerAddress of given PeerID from the DHT.
	// It wouldn't return any error if the PeerAddress doesn't exist.
	RemovePeerAddress(protocol.PeerID) error

	// AddPeerGroup creates a new PeerGroup in the dht with given name and PeerIDs.
	AddPeerGroup(protocol.PeerGroupID, protocol.PeerIDs) error

	// PeerGroup returns the PeerIDs with the given PeerGroupID. It returns an
	// ErrPeerNotFound if the PeerGroupID cannot be found.
	PeerGroup(protocol.PeerGroupID) (protocol.PeerIDs, protocol.PeerAddresses, error)

	// Remove a PeerGroup with given name from the DHT.
	RemovePeerGroup(protocol.PeerGroupID)
}

type dht struct {
	me    protocol.PeerAddress
	codec protocol.PeerAddressCodec
	store kv.Table

	groupsMu *sync.RWMutex
	groups   map[protocol.PeerGroupID]protocol.PeerIDs

	inMemCacheMu *sync.RWMutex
	inMemCache   map[string]protocol.PeerAddress
}

// New DHT that stores peer addresses in the given store. It will cache all peer
// addresses in memory for fast access. It is safe for concurrent use,
// regardless of the underlying store.
func New(me protocol.PeerAddress, codec protocol.PeerAddressCodec, store kv.Table, bootstrapAddrs ...protocol.PeerAddress) (DHT, error) {
	// fixme : check parameters are not nil
	dht := &dht{
		me:    me,
		codec: codec,
		store: store,

		groupsMu: new(sync.RWMutex),
		groups:   map[protocol.PeerGroupID]protocol.PeerIDs{},

		inMemCacheMu: new(sync.RWMutex),
		inMemCache:   map[string]protocol.PeerAddress{},
	}

	if err := dht.fillInMemCache(); err != nil {
		return nil, err
	}

	if count, err := dht.NumPeers(); count == 0 || err != nil {
		for _, addr := range bootstrapAddrs {
			if addr.Equal(me) {
				continue
			}
			if err := dht.addPeerAddressWithoutLock(addr); err != nil {
				return nil, fmt.Errorf("error adding bootstrap addresses: %v", err)
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

func (dht *dht) PeerAddress(id protocol.PeerID) (protocol.PeerAddress, error) {
	dht.inMemCacheMu.RLock()
	defer dht.inMemCacheMu.RUnlock()

	peerAddr, ok := dht.inMemCache[id.String()]
	if !ok {
		return nil, NewErrPeerNotFound(id)
	}
	return peerAddr, nil
}

func (dht *dht) UpdatePeerAddress(peerAddr protocol.PeerAddress) (bool, error) {
	dht.inMemCacheMu.Lock()
	defer dht.inMemCacheMu.Unlock()

	prevPeerAddr, ok := dht.inMemCache[peerAddr.PeerID().String()]
	if ok && !peerAddr.IsNewer(prevPeerAddr) {
		return false, nil
	}

	err := dht.addPeerAddressWithoutLock(peerAddr)
	return err == nil, err
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

func (dht *dht) AddPeerGroup(id protocol.PeerGroupID, ids protocol.PeerIDs) error {
	if id.Equal(protocol.NilPeerGroupID) {
		return protocol.ErrInvalidPeerGroupID
	}

	dht.groupsMu.Lock()
	defer dht.groupsMu.Unlock()

	dht.groups[id] = ids
	return nil
}

func (dht *dht) PeerGroup(id protocol.PeerGroupID) (protocol.PeerIDs, protocol.PeerAddresses, error) {
	// If GroupID is nil in which case we want to get all the peers
	if id.Equal(protocol.NilPeerGroupID) {
		addrs, err := dht.PeerAddresses()
		if err != nil {
			return nil, nil, err
		}
		ids := make([]protocol.PeerID, len(addrs))
		for i := range ids {
			ids[i] = addrs[i].PeerID()
		}
		return ids, addrs, nil
	}

	// Fetch Group details from the storage.
	peerIDs, ok := dht.groupPeerIDs(id)
	if !ok {
		return nil, nil, NewErrPeerGroupNotFound(id)
	}

	dht.inMemCacheMu.RLock()
	defer dht.inMemCacheMu.RUnlock()

	addrs := make(protocol.PeerAddresses, len(peerIDs))
	for i, id := range peerIDs {
		addrs[i] = dht.inMemCache[id.String()]
	}

	return peerIDs, addrs, nil
}

func (dht *dht) RemovePeerGroup(id protocol.PeerGroupID) {
	dht.groupsMu.Lock()
	defer dht.groupsMu.Unlock()

	delete(dht.groups, id)
}

func (dht *dht) groupPeerIDs(id protocol.PeerGroupID) (protocol.PeerIDs, bool) {
	dht.groupsMu.RLock()
	defer dht.groupsMu.RUnlock()

	peerIDs, ok := dht.groups[id]
	return peerIDs, ok
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
	iter := dht.store.Iterator()
	for iter.Next() {
		var data []byte
		if err := iter.Value(&data); err != nil {
			return fmt.Errorf("error scanning dht iterator: %v", err)
		}
		peerAddr, err := dht.codec.Decode(data)
		if err != nil {
			return fmt.Errorf("error decoding peerAddress: %v", err)
		}
		dht.inMemCache[peerAddr.PeerID().String()] = peerAddr
	}
	return nil
}

type ErrPeerNotFound struct {
	error
	protocol.PeerID
}

func NewErrPeerNotFound(peerID protocol.PeerID) error {
	return ErrPeerNotFound{
		error:  fmt.Errorf("peer=%v not found", peerID),
		PeerID: peerID,
	}
}

type ErrPeerGroupNotFound struct {
	error
	protocol.PeerGroupID
}

func NewErrPeerGroupNotFound(groupID protocol.PeerGroupID) error {
	return ErrPeerGroupNotFound{
		error:       fmt.Errorf("peer group=%v not found", groupID),
		PeerGroupID: groupID,
	}
}
