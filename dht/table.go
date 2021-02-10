package dht

import (
	"sort"
	"sync"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// Force InMemTable to implement the Table interface.
var _ Table = &InMemTable{}

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

	// AddPeer to the table with an associate network address.
	AddIP(id.Signatory, string)
	// DeleteIP from the table.
	DeleteIP(id.Signatory)
	// IP returns the network ip address associated with the given peer.
	IP(id.Signatory) (string, bool)

	// Peers returns the n closest peers to the local peer, using XORing as the
	// measure of distance between two peers.
	Peers(int) []id.Signatory
	// NumPeers returns the total number of peers with associated network
	// addresses in the table.
	NumPeers() int

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

	ipBySignatoryMu *sync.Mutex
	ipBySignatory   map[id.Signatory]string

	subnetsByHashMu *sync.Mutex
	subnetsByHash   map[id.Hash][]id.Signatory
}

func NewInMemTable(self id.Signatory) *InMemTable {
	return &InMemTable{
		self: self,

		sortedMu: new(sync.Mutex),
		sorted:   []id.Signatory{},

		addrsBySignatoryMu: new(sync.Mutex),
		addrsBySignatory:   map[id.Signatory]wire.Address{},

		ipBySignatoryMu: new(sync.Mutex),
		ipBySignatory:   map[id.Signatory]string{},

		subnetsByHashMu: new(sync.Mutex),
		subnetsByHash:   map[id.Hash][]id.Signatory{},
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

	_, ok := table.addrsBySignatory[peerID]
	if ok && table.self.Equal(&peerID) {
		return
	}

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

func (table *InMemTable) AddIP(peerID id.Signatory, ipAddress string) {
	table.ipBySignatoryMu.Lock()
	defer table.ipBySignatoryMu.Unlock()

	table.ipBySignatory[peerID] = ipAddress
}

func (table *InMemTable) DeleteIP(peerID id.Signatory) {
	table.ipBySignatoryMu.Lock()
	defer table.ipBySignatoryMu.Unlock()

	delete(table.ipBySignatory, peerID)
}

func (table *InMemTable) IP(peerID id.Signatory) (string, bool) {
	table.ipBySignatoryMu.Lock()
	defer table.ipBySignatoryMu.Unlock()

	ip, ok := table.ipBySignatory[peerID]
	return ip, ok
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

func (table *InMemTable) NumPeers() int {
	table.addrsBySignatoryMu.Lock()
	defer table.addrsBySignatoryMu.Unlock()

	return len(table.addrsBySignatory)
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
