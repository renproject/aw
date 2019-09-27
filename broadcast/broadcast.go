package broadcast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
	"github.com/sirupsen/logrus"
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
	store    kv.Table
	messages protocol.MessageSender
	events   protocol.EventSender
	logger   logrus.FieldLogger
}

// NewBroadcaster returns a Broadcaster that will use the given Storage
// interface and DHT interface for storing messages and peer addresses
// respectively.
func NewBroadcaster(store kv.Table, messages protocol.MessageSender, events protocol.EventSender, logger logrus.FieldLogger) Broadcaster {
	return &broadcaster{
		store:    store,
		messages: messages,
		events:   events,
		logger:   logger,
	}
}

// Broadcast a message to multiple remote servers in an attempt to saturate the
// network.
func (broadcaster *broadcaster) Broadcast(ctx context.Context, body protocol.MessageBody) error {
	messageWire := protocol.MessageOnTheWire{
		Message: protocol.NewMessage(protocol.V1, protocol.Broadcast, body),
	}
	select {
	case <-ctx.Done():
		return newErrBroadcasting(fmt.Errorf("error sending: %v", ctx.Err()))
	case broadcaster.messages <- messageWire:
	}
	return nil
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
	ok, err := broadcaster.messageHashAlreadySeen(messageHash)
	if err != nil {
		return newErrBroadcastInternal(fmt.Errorf("error getting message hash=%v: %v", messageHash, err))
	}
	if ok {
		// Ignore messages that have already been seen
		return nil
	}
	if err := broadcaster.insertMessageHash(messageHash); err != nil {
		return newErrBroadcastInternal(fmt.Errorf("error inserting message hash=%v: %v", messageHash, err))
	}

	// Emit an event for this newly seen message
	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}
	select {
	case <-ctx.Done():
		return newErrAcceptingBroadcast(fmt.Errorf("error receiving: %v", ctx.Err()))
	case broadcaster.events <- event:
	}

	// Re-broadcasting the message will downgrade its version to the version
	// supported by this broadcaster
	return broadcaster.Broadcast(ctx, message.Body)
}

func (broadcaster *broadcaster) insertMessageHash(hash protocol.MessageHash) error {
	return broadcaster.store.Insert(hash.String(), true)
}

func (broadcaster *broadcaster) messageHashAlreadySeen(hash protocol.MessageHash) (bool, error) {
	var exists bool
	if err := broadcaster.store.Get(hash.String(), &exists); err != nil && err.Error() != kv.ErrKeyNotFound.Error() {
		return false, err
	}
	return exists, nil
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

// ErrBroadcasting is returned when there is an error when broadcasting.
type ErrBroadcasting struct {
	error
}

func newErrBroadcasting(err error) error {
	return ErrBroadcasting{
		error: fmt.Errorf("broadcasting canceled: %v", err),
	}
}

// ErrAcceptingBroadcast is returned when there is an error when accepting a
// broadcast.
type ErrAcceptingBroadcast struct {
	error
}

func newErrAcceptingBroadcast(err error) error {
	return ErrAcceptingBroadcast{
		error: fmt.Errorf("accepting broadcast canceled: %v", err),
	}
}
