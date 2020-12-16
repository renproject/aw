package dht

import (
	"sort"
	"sync"

	"github.com/renproject/id"
)

// Force InMemTable to implement the Table interface.
var _ Table = &InMemTable{}

type Table interface {
	AddPeer(peerID id.Signatory, peerAddr string) bool
	DeletePeer(peerID id.Signatory)
	PeerAddress(peerID id.Signatory) (string, bool)
	Addresses(int) []id.Signatory
	NumPeers() int

	AddSubnet(signatories []id.Signatory) id.Hash
	DeleteSubnet(hash id.Hash)
	Subnet(hash id.Hash) []id.Signatory
}

type InMemTable struct {
	self id.Signatory

	signatoriesSortedMu *sync.Mutex
	signatoriesSorted   []id.Signatory

	addrsBySignatoryMu *sync.Mutex
	addrsBySignatory   map[id.Signatory]string
	signatoriesByAddr  map[string]id.Signatory

	subnetsByHashMu *sync.Mutex
	subnetsByHash   map[id.Hash][]id.Signatory
}

func NewInMemTable(self id.Signatory) *InMemTable {
	return &InMemTable{
		self : self,

		signatoriesSortedMu: new(sync.Mutex),
		signatoriesSorted:   []id.Signatory{},

		addrsBySignatoryMu: new(sync.Mutex),
		addrsBySignatory:   map[id.Signatory]string{},
		signatoriesByAddr:  map[string]id.Signatory{},

		subnetsByHashMu: new(sync.Mutex),
		subnetsByHash:   map[id.Hash][]id.Signatory{},
	}
}

func (table *InMemTable) AddPeer(peerID id.Signatory, peerAddr string) bool {
	table.addrsBySignatoryMu.Lock()
	table.signatoriesSortedMu.Lock()

	defer table.addrsBySignatoryMu.Unlock()
	defer table.signatoriesSortedMu.Unlock()

	if peerID.Equal(&table.self) {
		return false
	}

	existingAddr, ok := table.addrsBySignatory[peerID]
	if ok {
		if peerAddr == existingAddr {
			// If the addresses are the same, then the inserted address is not
			// new.
			return false
		}
	}

	// Insert into the map to allow for address lookup using the signatory.
	table.addrsBySignatory[peerID] = peerAddr

	// Insert into the sorted address list based on its XOR distance from our
	// own address.
	i := sort.Search(len(table.signatoriesSorted), func(i int) bool {
		return table.isCloser(peerID, table.signatoriesSorted[i])
	})
	table.signatoriesSorted = append(table.signatoriesSorted, id.Signatory{})
	copy(table.signatoriesSorted[i+1:], table.signatoriesSorted[i:])
	table.signatoriesSorted[i] = peerID
	return true
}

func (table *InMemTable) DeletePeer(peerID id.Signatory) {
	table.addrsBySignatoryMu.Lock()
	table.signatoriesSortedMu.Lock()

	defer table.addrsBySignatoryMu.Unlock()
	defer table.signatoriesSortedMu.Unlock()

	// Delete from the map.
	delete(table.addrsBySignatory, peerID)

	// Delete from the sorted list.
	numAddrs := len(table.signatoriesSorted)
	i := sort.Search(numAddrs, func(i int) bool {
		return table.isCloser(peerID, table.signatoriesSorted[i])
	})

	removeIndex := i - 1
	if removeIndex >= 0 {
		expectedID := table.signatoriesSorted[removeIndex]
		if expectedID.Equal(&peerID) {
			table.signatoriesSorted = append(table.signatoriesSorted[:removeIndex], table.signatoriesSorted[removeIndex+1:]...)
		}
	}
}

func (table *InMemTable) PeerAddress(peerID id.Signatory) (string, bool) {
	table.addrsBySignatoryMu.Lock()
	defer table.addrsBySignatoryMu.Unlock()

	addr, ok := table.addrsBySignatory[peerID]
	return addr, ok
}

// Addresses takes input `n` and returns the first `n` signatories in the sorted array of signatories it maintains.
// This is an O(n) operation as it copies the first min(n, len(sortedArrayOfSignatories)) signatories into a newly allocated
// array and returns it
func (table *InMemTable) Addresses(n int) []id.Signatory {
	table.addrsBySignatoryMu.Lock()
	defer table.addrsBySignatoryMu.Unlock()

	if n <= 0 {
		// For values of n that are less than, or equal to, zero, return an
		// empty list. We could panic instead, but this is a reasonable and
		// unsurprising alternative.
		return []id.Signatory{}
	}

	sigs := make([]id.Signatory, min(n, len(table.signatoriesSorted)))
	copy(sigs, table.signatoriesSorted)
	return sigs
}

func (table *InMemTable) NumPeers() int {
	table.addrsBySignatoryMu.Lock()
	defer table.addrsBySignatoryMu.Unlock()

	return len(table.addrsBySignatory)
}

func (table *InMemTable) AddSubnet(signatories []id.Signatory) id.Hash {
	copied := make([]id.Signatory, len(signatories))
	copy(copied, signatories)

	// Sort signatories in order of their XOR distance from our own address.
	sort.Slice(copied, func(i, j int) bool {
		return table.isCloser(copied[i], copied[j])
	})

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
