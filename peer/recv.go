package peer

import (
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// A Receiver is used to emit events about messages when they are received. Most
// high-level functionality, such as peer discovery, gossiping, and
// synchronisation, is implemented through the Receiver interface.
type Receiver interface {
	DidReceiveMessage(from id.Signatory, msg wire.Msg)
}

// Callbacks implements the Receiver interface by delegating all functionality
// to callback functions. It is useful for defining inline anyonmously typed
// Receivers.
type Callbacks struct {
	OnDidReceiveMessage func(id.Signatory, wire.Msg)
}

// DidReceiveMessage delegates all functionality to the OnDidReceiveMessage
// callback. If the callback is nil, this method does nothing.
func (cb Callbacks) DidReceiveMessage(from id.Signatory, msg wire.Msg) {
	if cb.OnDidReceiveMessage == nil {
		return
	}
	cb.OnDidReceiveMessage(from, msg)
}

// All composes multiple Receivers together and emits every event to all
// Receivers in order.
func All(receivers ...Receiver) Receiver {
	return Callbacks{
		OnDidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
			for _, receiver := range receivers {
				receiver.DidReceiveMessage(from, msg)
			}
		},
	}
}
