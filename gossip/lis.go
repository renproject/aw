package gossip

import "github.com/renproject/id"

type ContentListener interface {
	DidReceiveContent(hash id.Hash, contentType uint8, content []byte)
}

type Listener interface {
	ContentListener
}

// Callbacks implements the Listener interface by deferring all logic to
// closures. Closures that are nil will be gracefully ignored.
type Callbacks struct {
	ReceiveContent func(hash id.Hash, contentType uint8, content []byte)
}

func (cb Callbacks) DidReceiveContent(hash id.Hash, contentType uint8, content []byte) {
	if cb.ReceiveContent == nil {
		return
	}
	cb.ReceiveContent(hash, contentType, content)
}
