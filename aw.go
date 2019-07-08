package aw

import (
	"github.com/renproject/aw/protocol"
)

type (
	Message         = protocol.Message
	MessageSender   = protocol.MessageSender
	MessageReceiver = protocol.MessageReceiver

	Event         = protocol.Event
	EventSender   = protocol.EventSender
	EventReceiver = protocol.EventReceiver

	PeerID           = protocol.PeerID
	PeerIDs          = protocol.PeerIDs
	PeerAddress      = protocol.PeerAddress
	PeerAddresses    = protocol.PeerAddresses
	PeerAddressCodec = protocol.PeerAddressCodec
)
