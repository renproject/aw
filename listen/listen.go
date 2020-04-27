package listen

import (
	"github.com/renproject/aw/wire"
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
	DidReceivePing(version uint8, data []byte, from id.Signatory) (wire.Message, error)
	DidReceivePingAck(version uint8, data []byte, from id.Signatory) error
}

// A PushListener is responsible for implementing content discovery by pushing
// notifications to its peers. Notifications should be as small as possible.
type PushListener interface {
	DidReceivePush(version uint8, data []byte, from id.Signatory) (wire.Message, error)
	DidReceivePushAck(version uint8, data []byte, from id.Signatory) error
}

// A PullListener is responsible for implementing content synchronisation by
// pulling content from its peers after receiving notifications about the
// content.
type PullListener interface {
	DidReceivePull(version uint8, data []byte, from id.Signatory) (wire.Message, error)
	DidReceivePullAck(version uint8, data []byte, from id.Signatory) error
}
