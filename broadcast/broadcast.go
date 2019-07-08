package broadcast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
)

type Broadcaster interface {
	Broadcast(ctx context.Context, body protocol.MessageBody) error
	AcceptBroadcast(ctx context.Context, message protocol.Message) error
}

type broadcaster struct {
	storage  Storage
	dht      dht.DHT
	messages protocol.MessageSender
	events   protocol.EventSender
}

func NewBroadcaster(storage Storage, dht dht.DHT, messages protocol.MessageSender, events protocol.EventSender) Broadcaster {
	return &broadcaster{
		storage:  storage,
		dht:      dht,
		messages: messages,
		events:   events,
	}
}

func (broadcaster *broadcaster) Broadcast(ctx context.Context, body protocol.MessageBody) error {
	peerAddrs, err := broadcaster.dht.PeerAddresses()
	if err != nil {
		return newErrBroadcastingMessage(err)
	}
	if peerAddrs == nil {
		return newErrBroadcastingMessage(fmt.Errorf("nil peer addresses"))
	}
	if len(peerAddrs) <= 0 {
		return newErrBroadcastingMessage(fmt.Errorf("empty peer addresses"))
	}

	// Using the messaging sending channel protects the multicaster from
	// cascading time outs, but will still capture back pressure
	for i := range peerAddrs {
		messageWire := protocol.MessageOnTheWire{
			To:      peerAddrs[i].NetworkAddress(),
			Message: protocol.NewMessage(protocol.V1, protocol.Broadcast, body),
		}
		select {
		case <-ctx.Done():
			err = newErrBroadcastingMessage(ctx.Err())
		case broadcaster.messages <- messageWire:
		}
	}

	// Return the last error
	return err
}

func (broadcaster *broadcaster) AcceptBroadcast(ctx context.Context, message protocol.Message) error {
	// TODO: Check for compatible message version.

	messageHash := message.Hash()
	ok, err := broadcaster.storage.MessageHash(messageHash)
	if err != nil {
		return newErrBroadcastingMessage(fmt.Errorf("message hash=%v not found: %v", messageHash, err))
	}
	if ok {
		// Ignore messages that have already been seen
		return nil
	}
	if err := broadcaster.storage.InsertMessageHash(messageHash); err != nil {
		return newErrBroadcastingMessage(fmt.Errorf("error inserting message hash=%v: %v", messageHash, err))
	}

	// Emit an event for this newly seen message
	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}
	select {
	case <-ctx.Done():
		return newErrBroadcastingMessage(ctx.Err())
	case broadcaster.events <- event:
	}

	// Re-broadcasting the message will downgrade its version to the version
	// supported by this broadcaster
	return broadcaster.Broadcast(ctx, message.Body)
}

type ErrBroadcastingMessage struct {
	error
}

func newErrBroadcastingMessage(err error) error {
	return ErrBroadcastingMessage{
		error: fmt.Errorf("error broadcasting: %v", err),
	}
}
