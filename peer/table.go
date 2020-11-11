package peer

import (
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
}

type InMemTable struct {
	peersMu *sync.Mutex
	peers   map[id.Signatory]string
}

func NewInMemTable() *InMemTable {
	return &InMemTable{
		peersMu: new(sync.Mutex),
		peers:   map[id.Signatory]string{},
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
