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

	// TODO : Maybe refrain from overwriting the key if it already exists
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
