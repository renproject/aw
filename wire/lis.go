package wire

import (
	"github.com/renproject/id"
)

type Listener interface {
	PingListener
	PushListener
	PullListener
}

// A PingListener is responsible for implementing peer discovery by pinging its
// peers (and responding to pings received from peers).
type PingListener interface {
	DidReceivePing(version Version, data []byte, from id.Signatory) (Message, error)
	DidReceivePingAck(version Version, data []byte, from id.Signatory) error
}

// A PushListener is responsible for implementing content discovery by pushing
// notifications to its peers. Notifications should be as small as possible.
type PushListener interface {
	DidReceivePush(version Version, data []byte, from id.Signatory) (Message, error)
	DidReceivePushAck(version Version, data []byte, from id.Signatory) error
}

// A PullListener is responsible for implementing content synchronisation by
// pulling content from its peers after receiving notifications about the
// content.
type PullListener interface {
	DidReceivePull(version Version, data []byte, from id.Signatory) (Message, error)
	DidReceivePullAck(version Version, data []byte, from id.Signatory) error
}

// Callbacks implements the PingListener, PushListener, and PullListener
// interfaces by deferring all logic to closures. Closures that are nil will be
// gracefully ignored.
type Callbacks struct {
	ReceivePing    func(version Version, data []byte, from id.Signatory) (Message, error)
	ReceivePingAck func(version Version, data []byte, from id.Signatory) error
	ReceivePush    func(version Version, data []byte, from id.Signatory) (Message, error)
	ReceivePushAck func(version Version, data []byte, from id.Signatory) error
	ReceivePull    func(version Version, data []byte, from id.Signatory) (Message, error)
	ReceivePullAck func(version Version, data []byte, from id.Signatory) error
}

func (cb Callbacks) DidReceivePing(version Version, data []byte, from id.Signatory) (Message, error) {
	if cb.ReceivePing == nil {
		return Message{Version: version, Type: PingAck, Data: []byte{}}, nil
	}
	return cb.ReceivePing(version, data, from)
}

func (cb Callbacks) DidReceivePingAck(version Version, data []byte, from id.Signatory) error {
	if cb.ReceivePingAck == nil {
		return nil
	}
	return cb.ReceivePingAck(version, data, from)
}

func (cb Callbacks) DidReceivePush(version Version, data []byte, from id.Signatory) (Message, error) {
	if cb.ReceivePush == nil {
		return Message{Version: version, Type: PushAck, Data: []byte{}}, nil
	}
	return cb.ReceivePush(version, data, from)
}

func (cb Callbacks) DidReceivePushAck(version Version, data []byte, from id.Signatory) error {
	if cb.ReceivePushAck == nil {
		return nil
	}
	return cb.ReceivePushAck(version, data, from)
}

func (cb Callbacks) DidReceivePull(version Version, data []byte, from id.Signatory) (Message, error) {
	if cb.ReceivePull == nil {
		return Message{Version: version, Type: PullAck, Data: []byte{}}, nil
	}
	return cb.ReceivePull(version, data, from)
}

func (cb Callbacks) DidReceivePullAck(version Version, data []byte, from id.Signatory) error {
	if cb.ReceivePullAck == nil {
		return nil
	}
	return cb.ReceivePullAck(version, data, from)
}
