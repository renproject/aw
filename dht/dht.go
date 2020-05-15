package dht

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

// An Identifiable type is any type that is able to return a hash that uniquely
// identifies it.
type Identifiable interface {
	Hash() id.Hash
}

// DHT defines a distributed hash table, used for storing addresses/content that
// have been discovered in the network. All DHT implementations must be safe for
// concurrent use. All DHT implementations must be able to marshal/unmarshal
// to/from binary.
type DHT interface {
	surge.Surger

	// InsertAddr into the DHT. Returns true if the address is new, otherwise
	// returns false.
	InsertAddr(id.Signatory, wire.Address) bool
	// DeleteAddr from the DHT.
	DeleteAddr(id.Signatory)
	// Addr returns the address associated with a signatory. If there is no
	// associated address, it returns false. Otherwise, it returns true.
	Addr(id.Signatory) (wire.Address, bool)
	// Addrs returns a random number of addresses.
	Addrs(int) map[id.Signatory]wire.Address

	// InsertContent into the DHT. This will override existing content, so it is
	// important to call the HasContent method to check whether or not you are
	// able to override existing content.
	InsertContent(id.Hash, []byte)
	// DeleteContent from the DHT.
	DeleteContent(id.Hash)
	// Content returns the content associated with a hash. If there is no
	// associated content, it returns false. Otherwise, it returns true.
	Content(id.Hash) ([]byte, bool)
	// HasContent returns true when there is content associated with the given
	// hash. Otherwise, it returns false. This is more efficiently for checking
	// existence than the Content method, because no bytes are copied.
	HasContent(id.Hash) bool
	// HasContent returns true when there empty/nil content associated with the
	// given hash. Otherwise, it returns false. This is more efficiently for
	// checking existence than the Content method, because no bytes are copied.
	// Note: not having content is different from having empty/nil content.
	HasEmptyContent(id.Hash) bool

	// AddSubnet to the DHT. Returns the merkle root hash of the subnet.
	AddSubnet([]id.Signatory) id.Hash
	// DeleteSubnet from the DHT by specifying its merkle root hash. Does
	// nothing if the subnet is not in the DHT.
	DeleteSubnet(id.Hash)
	// Subnet returns the signatories associated with the specified subnet
	// merkle root hash.
	Subnet(id.Hash) []id.Signatory
}

type distributedHashTable struct {
	identity id.Signatory

	addrsBySignatoryMu *sync.Mutex
	addrsBySignatory   map[id.Signatory]wire.Address

	contentByHashMu *sync.Mutex
	contentByHash   map[id.Hash][]byte

	subnetsByHashMu *sync.Mutex
	subnetsByHash   map[id.Hash][]id.Signatory
}

// New returns an empty DHT.
func New(identity id.Signatory) DHT {
	return &distributedHashTable{
		identity: identity,

		addrsBySignatoryMu: new(sync.Mutex),
		addrsBySignatory:   map[id.Signatory]wire.Address{},

		contentByHashMu: new(sync.Mutex),
		contentByHash:   map[id.Hash][]byte{},

		subnetsByHashMu: new(sync.Mutex),
		subnetsByHash:   map[id.Hash][]id.Signatory{},
	}
}

// InsertAddr into the DHT. Returns true if the address is new, otherwise
// returns false.
func (dht *distributedHashTable) InsertAddr(signatory id.Signatory, addr wire.Address) bool {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	existingAddr, ok := dht.addrsBySignatory[signatory]
	if ok {
		if addr.Equal(&existingAddr) {
			// If the addresses are the same, then the inserted address is not
			// new.
			return false
		}
		if addr.Nonce <= existingAddr.Nonce {
			// If the inserted address does not have a greater nonce than the
			// existing address, we ignore the inserted address. This means we
			// have not inserted a new address, so we must return false.
			return false
		}
	}

	dht.addrsBySignatory[signatory] = addr
	return true
}

// DeleteAddr from the DHT.
func (dht *distributedHashTable) DeleteAddr(signatory id.Signatory) {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	delete(dht.addrsBySignatory, signatory)
}

// Addr returns the address associated with a signatory. If there is no
// associated address, it returns false. Otherwise, it returns true.
func (dht *distributedHashTable) Addr(signatory id.Signatory) (wire.Address, bool) {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	addr, ok := dht.addrsBySignatory[signatory] // This is safe, because addresses are cloned by default.
	return addr, ok
}

// Addrs returns a random number of addresses.
func (dht *distributedHashTable) Addrs(n int) map[id.Signatory]wire.Address {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	if n <= 0 {
		// For values of n that are less than, or equal to, zero, return an
		// empty map. We could panic instead, but this is a reasonable and
		// unsurprising alternative.
		return map[id.Signatory]wire.Address{}
	}

	addrsBySignatory := make(map[id.Signatory]wire.Address, n)
	for signatory, addr := range dht.addrsBySignatory {
		addrsBySignatory[signatory] = addr // This is safe, because addresses are cloned by default.
		if n--; n == 0 {
			break
		}
	}
	return addrsBySignatory
}

// InsertContent into the DHT. Returns true if there is not already content
// associated with this hash, otherwise returns false.
func (dht *distributedHashTable) InsertContent(hash id.Hash, content []byte) {
	dht.contentByHashMu.Lock()
	defer dht.contentByHashMu.Unlock()

	copied := make([]byte, len(content))
	copy(copied, content)
	dht.contentByHash[hash] = copied
}

// DeleteContent from the DHT.
func (dht *distributedHashTable) DeleteContent(hash id.Hash) {
	dht.contentByHashMu.Lock()
	defer dht.contentByHashMu.Unlock()

	delete(dht.contentByHash, hash)
}

// Content returns the content associated with a hash. If there is no
// associated content, it returns false. Otherwise, it returns true.
func (dht *distributedHashTable) Content(hash id.Hash) ([]byte, bool) {
	dht.contentByHashMu.Lock()
	defer dht.contentByHashMu.Unlock()

	content, ok := dht.contentByHash[hash]
	copied := make([]byte, len(content))
	if content != nil {
		copy(copied, content)
	}

	return content, ok
}

// HasContent returns the return when there is content associated with the
// given hash. Otherwise, it returns false. This is more efficiently for
// checking existence than the Content method, because no bytes are copied.
func (dht *distributedHashTable) HasContent(hash id.Hash) bool {
	dht.contentByHashMu.Lock()
	defer dht.contentByHashMu.Unlock()

	_, ok := dht.contentByHash[hash]
	return ok
}

// HasContent returns true when there empty/nil content associated with the
// given hash. Otherwise, it returns false. This is more efficiently for
// checking existence than the Content method, because no bytes are copied.
// Note: not having content is different from having empty/nil content.
func (dht *distributedHashTable) HasEmptyContent(hash id.Hash) bool {
	dht.contentByHashMu.Lock()
	defer dht.contentByHashMu.Unlock()

	content, ok := dht.contentByHash[hash]
	return ok && len(content) == 0
}

func (dht *distributedHashTable) AddSubnet(signatories []id.Signatory) id.Hash {
	copied := make([]id.Signatory, len(signatories))
	copy(copied, signatories)
	sort.Slice(copied, func(i, j int) bool {
		for b := 0; b < 32; b++ {
			d1 := dht.identity[b] ^ copied[i][b]
			d2 := dht.identity[b] ^ copied[j][b]
			if d1 < d2 {
				return true
			}
			if d2 < d1 {
				return false
			}
		}
		return false
	})
	hash := id.NewMerkleHashFromSignatories(copied)

	dht.subnetsByHashMu.Lock()
	defer dht.subnetsByHashMu.Unlock()

	dht.subnetsByHash[hash] = copied
	return hash
}

func (dht *distributedHashTable) DeleteSubnet(hash id.Hash) {
	dht.subnetsByHashMu.Lock()
	defer dht.subnetsByHashMu.Unlock()

	delete(dht.subnetsByHash, hash)
}

func (dht *distributedHashTable) Subnet(hash id.Hash) []id.Signatory {
	dht.subnetsByHashMu.Lock()
	defer dht.subnetsByHashMu.Unlock()

	subnet, ok := dht.subnetsByHash[hash]
	if !ok {
		return []id.Signatory{}
	}
	copied := make([]id.Signatory, len(subnet))
	copy(copied, subnet)
	return copied
}

// SizeHint returns the number of bytes required to represent this
// distributedHashTable in binary.
func (dht *distributedHashTable) SizeHint() int {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	return surge.SizeHint(dht.addrsBySignatory) + surge.SizeHint(dht.contentByHash)
}

// Marshal this distributedHashTable into binary.
func (dht *distributedHashTable) Marshal(w io.Writer, m int) (int, error) {
	dht.addrsBySignatoryMu.Lock()
	dht.contentByHashMu.Lock()
	dht.subnetsByHashMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()
	defer dht.contentByHashMu.Unlock()
	defer dht.subnetsByHashMu.Unlock()

	m, err := surge.Marshal(w, dht.addrsBySignatory, m)
	if err != nil {
		return m, fmt.Errorf("marshaling addresses by signatory: %v", err)
	}
	m, err = surge.Marshal(w, dht.contentByHash, m)
	if err != nil {
		return m, fmt.Errorf("marshaling content by hash: %v", err)
	}
	m, err = surge.Marshal(w, dht.subnetsByHash, m)
	if err != nil {
		return m, fmt.Errorf("marshaling subnets by hash: %v", err)
	}

	return m, nil
}

// Unmarshal from binary into this distributedHashTable.
func (dht *distributedHashTable) Unmarshal(r io.Reader, m int) (int, error) {
	dht.addrsBySignatoryMu.Lock()
	dht.contentByHashMu.Lock()
	dht.subnetsByHashMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()
	defer dht.contentByHashMu.Unlock()
	defer dht.subnetsByHashMu.Unlock()

	m, err := surge.Unmarshal(r, &dht.addrsBySignatory, m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling addresses by signatory: %v", err)
	}
	m, err = surge.Unmarshal(r, &dht.contentByHash, m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling content by hash: %v", err)
	}
	m, err = surge.Unmarshal(r, &dht.subnetsByHash, m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling subnets by hash: %v", err)
	}

	return m, nil
}
