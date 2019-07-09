package broadcast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
)

// A Broadcaster is used to send messages to all peers in the network. This is
// done using a decentralised gossip algorithm to ensure that a small number of
// malicioius peers cannot stop the message from saturating non-malicious peers.
//
// In V1, when a Broadcaster accepts a message it will hash it and check to see
// if it has seen this hash before. If the hash has been seen, nothing happens.
// If the hash has not been seen, the Broadcaster emits and event and propagates
// the message to all known peers.
type Broadcaster interface {
	// Broadcast a message to all peers in the network.
	Broadcast(ctx context.Context, body protocol.MessageBody) error

	// AcceptBroadcast message from another peer in the network.
	AcceptBroadcast(ctx context.Context, message protocol.Message) error
}

type broadcaster struct {
	storage  Storage
	dht      dht.DHT
	messages protocol.MessageSender
	events   protocol.EventSender
}

// NewBroadcaster returns a Broadcaster that will use the given Storage
// interface and DHT interface for storing messages and peer addresses
// respectively.
func NewBroadcaster(storage Storage, dht dht.DHT, messages protocol.MessageSender, events protocol.EventSender) Broadcaster {
	return &broadcaster{
		storage:  storage,
		dht:      dht,
		messages: messages,
		events:   events,
	}
}

// Broadcast a message to multiple remote servers in an attempt to saturate the
// network.
func (broadcaster *broadcaster) Broadcast(ctx context.Context, body protocol.MessageBody) error {
	peerAddrs, err := broadcaster.dht.PeerAddresses()
	if err != nil {
		return newErrBroadcastInternal(err)
	}
	if peerAddrs == nil {
		return newErrBroadcastInternal(fmt.Errorf("nil peer addresses"))
	}
	if len(peerAddrs) <= 0 {
		return newErrBroadcastInternal(fmt.Errorf("empty peer addresses"))
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
			err = newErrBroadcastCanceled(ctx.Err())
		case broadcaster.messages <- messageWire:
		}
	}

	// Return the last error
	return err
}

// AcceptBroadcast from a remote client and propagate it to all peers in the
// network.
func (broadcaster *broadcaster) AcceptBroadcast(ctx context.Context, message protocol.Message) error {
	// Pre-condition checks
	if message.Version != protocol.V1 {
		return newErrBroadcastVersionNotSupported(message.Version)
	}
	if message.Variant != protocol.Broadcast {
		return newErrBroadcastVariantNotSupported(message.Variant)
	}

	messageHash := message.Hash()
	ok, err := broadcaster.storage.MessageHash(messageHash)
	if err != nil {
		return newErrBroadcastInternal(fmt.Errorf("error loading message hash=%v: %v", messageHash, err))
	}
	if ok {
		// Ignore messages that have already been seen
		return nil
	}
	if err := broadcaster.storage.InsertMessageHash(messageHash); err != nil {
		return newErrBroadcastInternal(fmt.Errorf("error inserting message hash=%v: %v", messageHash, err))
	}

	// Emit an event for this newly seen message
	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}
	select {
	case <-ctx.Done():
		return newErrBroadcastCanceled(ctx.Err())
	case broadcaster.events <- event:
	}

	// Re-broadcasting the message will downgrade its version to the version
	// supported by this broadcaster
	return broadcaster.Broadcast(ctx, message.Body)
}

// ErrBroadcastInternal is returned when there is an internal broadcasting
// error. For example, when an error is returned by the underlying storage
// implementation.
type ErrBroadcastInternal struct {
	error
}

func newErrBroadcastInternal(err error) error {
	return ErrBroadcastInternal{
		error: fmt.Errorf("internal broadcast error: %v", err),
	}
}

// ErrBroadcastVersionNotSupported is returned when a broadcast message has an
// unsupported version.
type ErrBroadcastVersionNotSupported struct {
	error
}

func newErrBroadcastVersionNotSupported(version protocol.MessageVersion) error {
	return ErrBroadcastVersionNotSupported{
		error: fmt.Errorf("broadcast version=%v not supported", version),
	}
}

// ErrBroadcastVariantNotSupported is returned when a broadcast message has an
// unsupported variant.
type ErrBroadcastVariantNotSupported struct {
	error
}

func newErrBroadcastVariantNotSupported(variant protocol.MessageVariant) error {
	return ErrBroadcastVariantNotSupported{
		error: fmt.Errorf("broadcast variant=%v not supported", variant),
	}
}

// ErrBroadcastCanceled is returned when a broadcast is canceled. This is
// usually caused by a context being done.
type ErrBroadcastCanceled struct {
	error
}

func newErrBroadcastCanceled(err error) error {
	return ErrBroadcastCanceled{
		error: fmt.Errorf("broadcast canceled: %v", err),
	}
}
