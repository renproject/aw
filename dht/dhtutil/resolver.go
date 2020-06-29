package dhtutil

import (
	"github.com/renproject/aw/dht"
	"github.com/renproject/id"
)

type channelResolver struct {
	insertCh  chan id.Hash
	deleteCh  chan id.Hash
	contentCh chan id.Hash
}

// NewChannelResolver returns a ContentResolver that writes to given channels
// when corresponding methods are called.
func NewChannelResolver(insertCh, deleteCh, contentCh chan id.Hash) dht.ContentResolver {
	return &channelResolver{
		insertCh:  insertCh,
		deleteCh:  deleteCh,
		contentCh: contentCh,
	}
}

func (r *channelResolver) Insert(hash id.Hash, contentType uint8, content []byte) {
	r.insertCh <- hash
}

func (r *channelResolver) Delete(hash id.Hash, contentType uint8) {
	r.deleteCh <- hash
}

func (r *channelResolver) Content(hash id.Hash, contentType uint8, syncRequired bool) ([]byte, bool) {
	r.contentCh <- hash
	return nil, true
}

type insertCallbackResolver struct {
	callback func([]byte)
}

// NewInsertCallbackResolver returns a ContentResolver that calls the given
// callback when it inserts data.
func NewInsertCallbackResolver(callback func([]byte)) dht.ContentResolver {
	inner := &insertCallbackResolver{
		callback: callback,
	}
	return dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), inner)
}

func (r *insertCallbackResolver) Insert(hash id.Hash, contentType uint8, content []byte) {
	r.callback(content)
}

func (r *insertCallbackResolver) Delete(hash id.Hash, contentType uint8) {
}

func (r *insertCallbackResolver) Content(hash id.Hash, contentType uint8, syncRequired bool) ([]byte, bool) {
	// This is irrelevant since we use a `NewDoubleCacheContentResolver`
	// wrapper.
	return nil, false
}
