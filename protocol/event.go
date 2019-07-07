package protocol

import "time"

type EventSender chan<- Event
type EventReceiver <-chan Event

type Event interface {
	IsEvent()
}

type EventPeerChanged struct {
	Time        time.Time
	PeerAddress PeerAddress
}

func (EventPeerChanged) IsEvent() {}

type EventMessageReceived struct {
	Time    time.Time
	Message MessageBody
}

func (EventMessageReceived) IsEvent() {}
