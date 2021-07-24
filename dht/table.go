package dht

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// Force InMemTable to implement the Table interface.
var _ Table = &InMemTable{}

type Expiry struct {
	minimumExpiryAge time.Duration
	timestamp        time.Time
}

// A Table is responsible for keeping tack of peers, their network addresses,
// and the subnet to which they belong.
type Table interface {
	// Self returns the local peer. It does not return the network address of
	// the local peer, because it can change frequently, and is not guaranteed
	// to exist.
	Self() id.Signatory

	// AddPeer to the table with an associate network address.
	AddPeer(id.Signatory, wire.Address)
	// DeletePeer from the table.
	DeletePeer(id.Signatory)
	// PeerAddress returns the network address associated with the given peer.
	PeerAddress(id.Signatory) (wire.Address, bool)

	// Peers returns the n closest peers to the local peer, using XORing as the
	// measure of distance between two peers.
	Peers(int) []id.Signatory
	// RandomPeers returns n random peer IDs, using either partial permutation
	// or Floyd's sampling algorithm.
	RandomPeers(int) []id.Signatory
	// NumPeers returns the total number of peers with associated network
	// addresses in the table.
	NumPeers() int

	// HandleExpired returns whether a signatory has expired. It checks whether
	// an Expiry exists for the signatory, and if it does, has it expired?
	// If found expired, it deletes the peer from the table
	HandleExpired(id.Signatory) bool
	// AddExpiry to the table with given duration if no existing expiry is found
	AddExpiry(id.Signatory, time.Duration)
	// DeleteExpiry from the table
	DeleteExpiry(id.Signatory)

	// AddSubnet to the table. This returns a subnet hash that can be used to
	// read/delete the subnet. It is the merkle root hash of the peers in the
	// subnet.
	AddSubnet([]id.Signatory) id.Hash
	// DeleteSubnet from the table. If the subnet was in the table, then the
	// peers are returned.
	DeleteSubnet(id.Hash)
	// Subnet returns the peers from the table.
	Subnet(id.Hash) []id.Signatory
}

// InMemTable implements the Table using in-memory storage.
type InMemTable struct {
	self id.Signatory

	sortedMu *sync.Mutex
	sorted   []id.Signatory

	addrsBySignatoryMu *sync.Mutex
	addrsBySignatory   map[id.Signatory]wire.Address

	expiryBySignatoryMu *sync.Mutex
	expiryBySignatory   map[id.Signatory]Expiry

	subnetsByHashMu *sync.Mutex
	subnetsByHash   map[id.Hash][]id.Signatory

	randObj *rand.Rand
}

func NewInMemTable(self id.Signatory) *InMemTable {
	return &InMemTable{
		self: self,

		sortedMu: new(sync.Mutex),
		sorted:   []id.Signatory{},

		addrsBySignatoryMu: new(sync.Mutex),
		addrsBySignatory:   map[id.Signatory]wire.Address{},

		expiryBySignatoryMu: new(sync.Mutex),
		expiryBySignatory:   map[id.Signatory]Expiry{},

		subnetsByHashMu: new(sync.Mutex),
		subnetsByHash:   map[id.Hash][]id.Signatory{},

		randObj: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (table *InMemTable) Self() id.Signatory {
	return table.self
}

func (table *InMemTable) AddPeer(peerID id.Signatory, peerAddr wire.Address) {
	table.sortedMu.Lock()
	table.addrsBySignatoryMu.Lock()

	defer table.sortedMu.Unlock()
	defer table.addrsBySignatoryMu.Unlock()

	if table.self.Equal(&peerID) {
		return
	}

	_, ok := table.addrsBySignatory[peerID]

	// Insert into the map to allow for address lookup using the signatory.
	table.addrsBySignatory[peerID] = peerAddr

	// Insert into the sorted signatories list based on its XOR distance from our
	// own address.
	if !ok {
		i := sort.Search(len(table.sorted), func(i int) bool {
			return table.isCloser(peerID, table.sorted[i])
		})
		table.sorted = append(table.sorted, id.Signatory{})
		copy(table.sorted[i+1:], table.sorted[i:])
		table.sorted[i] = peerID
	}
}

func (table *InMemTable) DeletePeer(peerID id.Signatory) {
	table.sortedMu.Lock()
	table.addrsBySignatoryMu.Lock()

	defer table.sortedMu.Unlock()
	defer table.addrsBySignatoryMu.Unlock()

	// Delete from the map.
	delete(table.addrsBySignatory, peerID)

	// Delete from the sorted list.
	numAddrs := len(table.sorted)
	i := sort.Search(numAddrs, func(i int) bool {
		return table.isCloser(peerID, table.sorted[i])
	})

	removeIndex := i - 1
	if removeIndex >= 0 {
		table.sorted = append(table.sorted[:removeIndex], table.sorted[removeIndex+1:]...)
	}
}

func (table *InMemTable) PeerAddress(peerID id.Signatory) (wire.Address, bool) {
	table.addrsBySignatoryMu.Lock()
	defer table.addrsBySignatoryMu.Unlock()

	addr, ok := table.addrsBySignatory[peerID]
	return addr, ok
}

// Peers returns the n closest peer IDs.
func (table *InMemTable) Peers(n int) []id.Signatory {
	table.sortedMu.Lock()
	defer table.sortedMu.Unlock()

	if n <= 0 {
		// For values of n that are less than, or equal to, zero, return an
		// empty list. We could panic instead, but this is a reasonable and
		// unsurprising alternative.
		return []id.Signatory{}
	}

	sigs := make([]id.Signatory, min(n, len(table.sorted)))
	copy(sigs, table.sorted)
	return sigs
}

// RandomPeers returns n random peer IDs
func (table *InMemTable) RandomPeers(n int) []id.Signatory {
	table.sortedMu.Lock()
	m := len(table.sorted)
	table.sortedMu.Unlock()

	if n <= 0 {
		// For values of n that are less than, or equal to, zero, return an
		// empty list. We could panic instead, but this is a reasonable and
		// unsurprising alternative.
		return []id.Signatory{}
	}
	if n >= m {
		sigs := make([]id.Signatory, m)
		table.sortedMu.Lock()
		defer table.sortedMu.Unlock()
		copy(sigs, table.sorted)
		return sigs
	}

	// Use the first n elements of a permutation of the entire list of peer IDs
	// This is used only if the sorted array (array of length m) is sufficiently
	// small or the number of random elements to be selected (n) i sufficiently
	// large in comparison to m
	if m <= 10000 || n >= m/50.0 {
		shuffled := make([]id.Signatory, n)
		indexPerm := rand.Perm(m)
		table.sortedMu.Lock()
		defer table.sortedMu.Unlock()
		for i := 0; i < n; i++ {
			shuffled[i] = table.sorted[indexPerm[i]]
		}
		return shuffled
	}

	// Otherwise, use Floyd's sampling algorithm to select n random elements
	set := make(map[int]struct{}, n)
	randomSelection := make([]id.Signatory, 0, n)
	table.sortedMu.Lock()
	defer table.sortedMu.Unlock()
	for i := m - n; i < m; i++ {
		index := table.randObj.Intn(i)
		if _, ok := set[index]; !ok {
			set[index] = struct{}{}
			randomSelection = append(randomSelection, table.sorted[index])
			continue
		}
		set[i] = struct{}{}
		randomSelection = append(randomSelection, table.sorted[i])
	}
	return randomSelection
}

func (table *InMemTable) NumPeers() int {
	table.addrsBySignatoryMu.Lock()
	defer table.addrsBySignatoryMu.Unlock()

	return len(table.addrsBySignatory)
}

func (table *InMemTable) HandleExpired(peerID id.Signatory) bool {
	table.expiryBySignatoryMu.Lock()
	defer table.expiryBySignatoryMu.Unlock()
	expiry, ok := table.expiryBySignatory[peerID]
	if !ok {
		return false
	}
	expired := (time.Now().Sub(expiry.timestamp)) > expiry.minimumExpiryAge
	if expired {
		table.DeletePeer(peerID)
		delete(table.expiryBySignatory, peerID)
	}
	return expired
}

func (table *InMemTable) AddExpiry(peerID id.Signatory, duration time.Duration) {
	table.expiryBySignatoryMu.Lock()
	defer table.expiryBySignatoryMu.Unlock()
	_, ok := table.PeerAddress(peerID)
	if !ok {
		return
	}
	_, ok = table.expiryBySignatory[peerID]
	if ok {
		return
	}
	table.expiryBySignatory[peerID] = Expiry{
		minimumExpiryAge: duration,
		timestamp:        time.Now(),
	}
}

func (table *InMemTable) DeleteExpiry(peerID id.Signatory) {
	table.expiryBySignatoryMu.Lock()
	defer table.expiryBySignatoryMu.Unlock()
	delete(table.expiryBySignatory, peerID)
}

func (table *InMemTable) AddSubnet(signatories []id.Signatory) id.Hash {
	copied := make([]id.Signatory, len(signatories))
	copy(copied, signatories)

	// Sort signatories in order of their XOR distance from the local peer. This
	// allows different peers to easily iterate over the same subnet in a
	// different order.
	sort.Slice(copied, func(i, j int) bool {
		return table.isCloser(copied[i], copied[j])
	})

	// It it important to note that we are using the unsorted slice for
	// computing the merkle root hash. This is done so that everyone can have
	// the same subnet ID for a given slice of signatories.
	hash := id.NewMerkleHashFromSignatories(signatories)

	table.subnetsByHashMu.Lock()
	defer table.subnetsByHashMu.Unlock()

	table.subnetsByHash[hash] = copied
	return hash
}

func (table *InMemTable) DeleteSubnet(hash id.Hash) {
	table.subnetsByHashMu.Lock()
	defer table.subnetsByHashMu.Unlock()

	delete(table.subnetsByHash, hash)
}

func (table *InMemTable) Subnet(hash id.Hash) []id.Signatory {
	table.subnetsByHashMu.Lock()
	defer table.subnetsByHashMu.Unlock()

	subnet, ok := table.subnetsByHash[hash]
	if !ok {
		return []id.Signatory{}
	}
	copied := make([]id.Signatory, len(subnet))
	copy(copied, subnet)
	return copied
}

func (table *InMemTable) isCloser(fst, snd id.Signatory) bool {
	for b := 0; b < 32; b++ {
		d1 := table.self[b] ^ fst[b]
		d2 := table.self[b] ^ snd[b]
		if d1 < d2 {
			return true
		}
		if d2 < d1 {
			return false
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
