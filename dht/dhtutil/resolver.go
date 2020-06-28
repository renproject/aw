package dhtutil

import (
	"github.com/renproject/aw/dht"
	"github.com/renproject/id"
)

type mockResolver struct {
	insertCh  chan id.Hash
	deleteCh  chan id.Hash
	contentCh chan id.Hash
}

// NewMockResolver returns a ContentResolver that writes to given channels when
// corresponding methods are called.
func NewMockResolver(insertCh, deleteCh, contentCh chan id.Hash) dht.ContentResolver {
	return &mockResolver{
		insertCh:  insertCh,
		deleteCh:  deleteCh,
		contentCh: contentCh,
	}
}

func (r *mockResolver) Insert(hash id.Hash, contentType uint8, content []byte) {
	r.insertCh <- hash
}

func (r *mockResolver) Delete(hash id.Hash) {
	r.deleteCh <- hash
}

func (r *mockResolver) Content(hash id.Hash, contentType uint8, syncRequired bool) ([]byte, bool) {
	r.contentCh <- hash
	return nil, true
}
