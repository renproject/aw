package listen

import (
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// Callbacks implements the PingListener, PushListener, and PullListener
// interfaces by deferring all logic to closures. Closures that are nil will be
// gracefully ignored.
type Callbacks struct {
	ReceivePing    func(version uint8, data []byte, from id.Signatory) (wire.Message, error)
	ReceivePingAck func(version uint8, data []byte, from id.Signatory) error
	ReceivePush    func(version uint8, data []byte, from id.Signatory) (wire.Message, error)
	ReceivePushAck func(version uint8, data []byte, from id.Signatory) error
	ReceivePull    func(version uint8, data []byte, from id.Signatory) (wire.Message, error)
	ReceivePullAck func(version uint8, data []byte, from id.Signatory) error
}

func (cb Callbacks) DidReceivePing(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if cb.ReceivePing == nil {
		return wire.Message{Version: version, Type: wire.PingAck, Data: []byte{}}, nil
	}
	return cb.ReceivePing(version, data, from)
}

func (cb Callbacks) DidReceivePingAck(version uint8, data []byte, from id.Signatory) error {
	if cb.ReceivePingAck == nil {
		return nil
	}
	return cb.ReceivePingAck(version, data, from)
}

func (cb Callbacks) DidReceivePush(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if cb.ReceivePush == nil {
		return wire.Message{Version: version, Type: wire.PushAck, Data: []byte{}}, nil
	}
	return cb.ReceivePush(version, data, from)
}

func (cb Callbacks) DidReceivePushAck(version uint8, data []byte, from id.Signatory) error {
	if cb.ReceivePushAck == nil {
		return nil
	}
	return cb.ReceivePushAck(version, data, from)
}

func (cb Callbacks) DidReceivePull(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if cb.ReceivePull == nil {
		return wire.Message{Version: version, Type: wire.PullAck, Data: []byte{}}, nil
	}
	return cb.ReceivePull(version, data, from)
}

func (cb Callbacks) DidReceivePullAck(version uint8, data []byte, from id.Signatory) error {
	if cb.ReceivePullAck == nil {
		return nil
	}
	return cb.ReceivePullAck(version, data, from)
}
