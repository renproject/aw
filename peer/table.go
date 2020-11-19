package peer

import (
	"sort"
	"sync"

	"github.com/renproject/id"
)

// Force InMemTable to implement the Table interface.
var _ Table = &InMemTable{}

type Table interface {
	AddPeer(peerID id.Signatory, peerAddr string)
	DeletePeer(peerID id.Signatory)
	PeerAddress(peerID id.Signatory) (string, bool)
	All() []id.Signatory
	AddSubnet(signatories []id.Signatory) id.Hash
	DeleteSubnet(hash id.Hash)
	Subnet(hash id.Hash) []id.Signatory
}

type InMemTable struct {
	self id.Signatory

	peersMu *sync.Mutex
	peers   map[id.Signatory]string

	subnetsByHashMu *sync.Mutex
	subnetsByHash   map[id.Hash][]id.Signatory
}

func NewInMemTable(self id.Signatory) *InMemTable {
	return &InMemTable{
		peersMu: new(sync.Mutex),
		peers:   map[id.Signatory]string{},
		subnetsByHashMu: new(sync.Mutex),
		subnetsByHash: map[id.Hash][]id.Signatory{},
	}
}

func (table *InMemTable) AddPeer(peerID id.Signatory, peerAddr string) {
	table.peersMu.Lock()
	defer table.peersMu.Unlock()

	table.peers[peerID] = peerAddr
}

func (table *InMemTable) DeletePeer(peerID id.Signatory) {
	table.peersMu.Lock()
	defer table.peersMu.Unlock()

	delete(table.peers, peerID)
}

func (table *InMemTable) PeerAddress(peerID id.Signatory) (string, bool) {
	table.peersMu.Lock()
	defer table.peersMu.Unlock()

	val, ok := table.peers[peerID]
	return val, ok
}

func (table *InMemTable) All() []id.Signatory {
	sigs := make([]id.Signatory, 0, len(table.peers))
	for k := range table.peers {
		sigs = append(sigs, k)
	}
	return sigs
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
