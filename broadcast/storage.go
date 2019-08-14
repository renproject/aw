package broadcast

import (
	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
)

type Storage interface {
	InsertMessageHash(protocol.MessageHash) error
	MessageHash(protocol.MessageHash) (bool, error)
}

type storage struct {
	store kv.Table
}

func NewStorage(store kv.Table) Storage {
	return &storage{store: store}
}

func (storage *storage) InsertMessageHash(hash protocol.MessageHash) error {
	return storage.store.Insert(hash.String(), true)
}

func (storage *storage) MessageHash(hash protocol.MessageHash) (bool, error) {
	var exists bool
	if err := storage.store.Get(hash.String(), &exists); err != nil && err != kv.ErrKeyNotFound {
		return false, err
	}
	return exists, nil
}
