package dht

import (
	"sort"
	"sync"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// An Identifiable type is any type that is able to return a hash that uniquely
// identifies it.
type Identifiable interface {
	Hash() id.Hash
}

// DHT defines a distributed hash table, used for storing addresses/content that
// have been discovered in the network. All DHT implementations must be safe for
// concurrent use.
type DHT interface {
	// InsertAddr into the DHT. Returns true if the address is new, otherwise
	// returns false.
	InsertAddr(wire.Address) bool
	// DeleteAddr from the DHT.
	DeleteAddr(id.Signatory)
	// Addr returns the address associated with a signatory. If there is no
	// associated address, it returns false. Otherwise, it returns true.
	Addr(id.Signatory) (wire.Address, bool)
	// Addrs returns a random subset of addresses in the store. The input
	// argument can be used to specify the maximum number of addresses returned.
	Addrs(int) []wire.Address
	// NumAddrs returns the number of addresses in the store.
	NumAddrs() (int, error)

	// InsertContent with the given type into the DHT. This will override
	// existing content, so it is important to call the HasContent method to
	// check whether or not you are able to override existing content.
	InsertContent(id.Hash, uint8, []byte)
	// DeleteContent from the DHT.
	DeleteContent(id.Hash, uint8)
	// Content returns the content associated with a hash. If there is no
	// associated content, it returns false. Otherwise, it returns true.
	Content(id.Hash, uint8) ([]byte, bool)
	// HasContent returns true when there is content associated with the given
	// hash. Otherwise, it returns false. This is more efficiently for checking
	// existence than the Content method, because no bytes are copied.
	HasContent(id.Hash, uint8) bool
	// HasContent returns true when there empty/nil content associated with the
	// given hash. Otherwise, it returns false. This is more efficiently for
	// checking existence than the Content method, because no bytes are copied.
	// Note: not having content is different from having empty/nil content.
	HasEmptyContent(id.Hash, uint8) bool

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
	identity        id.Signatory
	contentResolver ContentResolver

	addrsSortedMu *sync.Mutex
	addrsSorted   []wire.Address

	addrsBySignatoryMu *sync.Mutex
	addrsBySignatory   map[id.Signatory]wire.Address
	signatoriesByAddr  map[wire.Address]id.Signatory

	subnetsByHashMu *sync.Mutex
	subnetsByHash   map[id.Hash][]id.Signatory
}

// New returns an empty DHT. If a nil content resolver is provided, the function
// will panic. A default double-cache content resolver can be used to store
// content in-memory if it is not required to persist across reboots.
//
// dht.New(identity, dht.NewDoubleCacheContentResolver(resolverOpts, nil))
//
// The default resolver can also act as a middleware by replacing the `nil`
// argument with a custom ContentResolver implementation.
func New(identity id.Signatory, contentResolver ContentResolver) DHT {
	if contentResolver == nil {
		panic("failed to construct dht: nil content resolver")
	}

	return &distributedHashTable{
		identity:        identity,
		contentResolver: contentResolver,

		addrsSortedMu: new(sync.Mutex),
		addrsSorted:   []wire.Address{},

		addrsBySignatoryMu: new(sync.Mutex),
		// addrsBySignatory:   hbm.New(),
		addrsBySignatory:  make(map[id.Signatory]wire.Address),
		signatoriesByAddr: make(map[wire.Address]id.Signatory),

		subnetsByHashMu: new(sync.Mutex),
		subnetsByHash:   map[id.Hash][]id.Signatory{},
	}
}

// InsertAddr into the DHT. Returns true if the address is new, otherwise
// returns false.
func (dht *distributedHashTable) InsertAddr(addr wire.Address) bool {
	dht.addrsBySignatoryMu.Lock()
	dht.addrsSortedMu.Lock()

	defer dht.addrsBySignatoryMu.Unlock()
	defer dht.addrsSortedMu.Unlock()

	signatory, err := addr.Signatory()
	if err != nil {
		// If there is an error fetching the signatory, return false.
		return false
	}

	if signatory.Equal(&dht.identity) {
		// Do not insert our own address into the DHT.
		return false
	}

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

	// Insert into the map to allow for address lookup using the signatory.
	dht.addrsBySignatory[signatory] = addr
	dht.signatoriesByAddr[addr] = signatory

	// Insert into the sorted address list based on its XOR distance from our
	// own address.
	i := sort.Search(len(dht.addrsSorted), func(i int) bool {
		currentSig, ok := dht.signatoriesByAddr[dht.addrsSorted[i]]
		if !ok {
			return false
		}

		return dht.isCloser(signatory, currentSig)
	})
	dht.addrsSorted = append(dht.addrsSorted, wire.Address{})
	copy(dht.addrsSorted[i+1:], dht.addrsSorted[i:])
	dht.addrsSorted[i] = addr

	return true
}

// DeleteAddr from the DHT.
func (dht *distributedHashTable) DeleteAddr(signatory id.Signatory) {
	dht.addrsBySignatoryMu.Lock()
	dht.addrsSortedMu.Lock()

	defer dht.addrsBySignatoryMu.Unlock()
	defer dht.addrsSortedMu.Unlock()

	// Delete from the sorted list.
	numAddrs := len(dht.addrsSorted)
	i := sort.Search(numAddrs, func(i int) bool {
		currentSig, ok := dht.signatoriesByAddr[dht.addrsSorted[i]]
		if !ok {
			return false
		}

		return dht.isCloser(signatory, currentSig)
	})

	toRemove := i - 1
	if toRemove >= 0 {
		expectedSig, ok := dht.signatoriesByAddr[dht.addrsSorted[toRemove]]
		if !ok {
			// This should not be possible as only addresses with valid
			// signatories are inserted into the DHT.
			return
		}

		if expectedSig.Equal(&signatory) {
			dht.addrsSorted = append(dht.addrsSorted[:toRemove], dht.addrsSorted[toRemove+1:]...)
		}
	}

	// Delete from the map.
	addr := dht.addrsBySignatory[signatory]
	delete(dht.signatoriesByAddr, addr)
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

// Addrs returns a subset of addresses in the store ordered by their XOR
// distance from our own address. The input argument can be used to specify the
// maximum number of addresses returned.
func (dht *distributedHashTable) Addrs(n int) []wire.Address {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	if n <= 0 {
		// For values of n that are less than, or equal to, zero, return an
		// empty list. We could panic instead, but this is a reasonable and
		// unsurprising alternative.
		return []wire.Address{}
	}

	addrs := make([]wire.Address, 0, n)
	for _, addr := range dht.addrsSorted {
		addrs = append(addrs, addr) // This is safe, because addresses are cloned by default.
		if n--; n == 0 {
			break
		}
	}

	return addrs
}

// NumAddrs returns the number of addresses in the store.
func (dht *distributedHashTable) NumAddrs() (int, error) {
	dht.addrsBySignatoryMu.Lock()
	defer dht.addrsBySignatoryMu.Unlock()

	return len(dht.addrsBySignatory), nil
}

// InsertContent into the DHT. Returns true if there is not already content
// associated with this hash, otherwise returns false.
func (dht *distributedHashTable) InsertContent(hash id.Hash, contentType uint8, content []byte) {
	dht.contentResolver.Insert(hash, contentType, content)
}

// DeleteContent from the DHT.
func (dht *distributedHashTable) DeleteContent(hash id.Hash, contentType uint8) {
	dht.contentResolver.Delete(hash, contentType)
}

// Content returns the content associated with a hash. If there is no
// associated content, it returns false. Otherwise, it returns true.
func (dht *distributedHashTable) Content(hash id.Hash, contentType uint8) ([]byte, bool) {
	return dht.contentResolver.Content(hash, contentType)
}

// HasContent returns true when there is content associated with the given hash.
// Otherwise, it returns false. This is more efficient for checking existence
// than the Content method, because no bytes are copied.
func (dht *distributedHashTable) HasContent(hash id.Hash, contentType uint8) bool {
	_, ok := dht.contentResolver.Content(hash, contentType)
	return ok
}

// HasContent returns true when there empty/nil content associated with the
// given hash. Otherwise, it returns false. This is more efficiently for
// checking existence than the Content method, because no bytes are copied.
// Note: not having content is different from having empty/nil content.
func (dht *distributedHashTable) HasEmptyContent(hash id.Hash, contentType uint8) bool {
	content, ok := dht.contentResolver.Content(hash, contentType)
	return ok && len(content) == 0
}

func (dht *distributedHashTable) AddSubnet(signatories []id.Signatory) id.Hash {
	copied := make([]id.Signatory, len(signatories))
	copy(copied, signatories)

	// Sort signatories in order of their XOR distance from our own address.
	sort.Slice(copied, func(i, j int) bool {
		return dht.isCloser(copied[i], copied[j])
	})

	hash := id.NewMerkleHashFromSignatories(signatories)

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

// isCloser compares two signatory addresses to our address and returns true if
// the first is closer in XOR distance than the second.
func (dht *distributedHashTable) isCloser(fst, snd id.Signatory) bool {
	for b := 0; b < 32; b++ {
		d1 := dht.identity[b] ^ fst[b]
		d2 := dht.identity[b] ^ snd[b]
		if d1 < d2 {
			return true
		}
		if d2 < d1 {
			return false
		}
	}
	return false
}
