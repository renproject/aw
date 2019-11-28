package protocol

import "time"

// EventSender is used for sending Event.
type EventSender chan<- Event

// EventReceiver is used for reading Event.
type EventReceiver <-chan Event

// Event is used to notify user when something happens.
type Event interface {
	IsEvent()
}

// EventPeerChanged is triggered when we detect an address change of a Peer.
type EventPeerChanged struct {
	Time        time.Time
	PeerAddress PeerAddress
}

// EventPeerChanged implements the Event interface.
func (EventPeerChanged) IsEvent() {}

// EventMessageReceived is triggered when we receive an AW message.
type EventMessageReceived struct {
	Time    time.Time
	Message MessageBody
	From    PeerID
}

// EventMessageReceived implements the Event interface.
func (EventMessageReceived) IsEvent() {}
