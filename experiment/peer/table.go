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
	panic("unimplemented")
}

func (table *InMemTable) DeletePeer(peerID id.Signatory) {
	panic("unimplemented")
}

func (table *InMemTable) PeerAddress(peerID id.Signatory) (string, bool) {
	panic("unimplemented")
}
