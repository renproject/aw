package broadcast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/id"
	"github.com/renproject/kv"
	"github.com/renproject/phi"
	"github.com/sirupsen/logrus"
)

// A Broadcaster is used to send messages to all peers in the network. This is
// done using a decentralised gossip algorithm to ensure that a small number of
// malicious peers cannot stop the message from saturating non-malicious peers.
//
// In V1, when a Broadcaster accepts a message it will hash it and check to see
// if it has seen this hash before. If the hash has been seen, nothing happens.
// If the hash has not been seen, the Broadcaster emits and event and propagates
// the message to all known peers.
type Broadcaster interface {
	// Broadcast a message to all peers in the network.
	Broadcast(ctx context.Context, groupID protocol.PeerGroupID, body protocol.MessageBody) error

	// AcceptBroadcast message from another peer in the network.
	AcceptBroadcast(ctx context.Context, from protocol.PeerID, message protocol.Message) error
}

type broadcaster struct {
	logger   logrus.FieldLogger
	store    kv.Table
	messages protocol.MessageSender
	events   protocol.EventSender
	dht      dht.DHT
}

// NewBroadcaster returns a Broadcaster that will use the given Storage
// interface and DHT interface for storing messages and peer addresses
// respectively.
func NewBroadcaster(logger logrus.FieldLogger, messages protocol.MessageSender, events protocol.EventSender, dht dht.DHT) Broadcaster {
	store := kv.NewTable(kv.NewMemDB(kv.GobCodec), "broadcaster")
	return &broadcaster{
		logger:   logger,
		store:    store,
		messages: messages,
		events:   events,
		dht:      dht,
	}
}

// Broadcast a message to multiple remote servers in an attempt to saturate the
// network.
func (broadcaster *broadcaster) Broadcast(ctx context.Context, groupID protocol.PeerGroupID, body protocol.MessageBody) error {
	// Ignore message if it already been sent.
	message := protocol.NewMessage(protocol.V1, protocol.Broadcast, groupID, body)
	ok, err := broadcaster.messageHashAlreadySeen(message.Hash())
	if err != nil {
		return newErrBroadcastInternal(fmt.Errorf("error getting message hash=%v: %v", message.Hash(), err))
	}
	if ok {
		return nil
	}

	// Get all peer details of the Group ID.
	ids, addrs, err := broadcaster.dht.PeerGroup(groupID)
	if err != nil {
		return err
	}

	// Check if context is already expired
	select {
	case <-ctx.Done():
		return newErrBroadcasting(ctx.Err(), groupID)
	default:
	}

	if err := broadcaster.store.Insert(message.Hash().String(), true); err != nil {
		return err
	}

	phi.ParForAll(addrs, func(i int) {
		to := addrs[i]
		if to == nil {
			broadcaster.logger.Debugf("cannot broadcast to node [%v] in group [%v], cannot find PeerAddress from dht.", ids[i], groupID)
			return
		}
		messageWire := protocol.MessageOnTheWire{
			To:      to,
			Message: message,
		}

		select {
		case <-ctx.Done():
			broadcaster.logger.Debugf("cannot send message to %v, %v", to.PeerID(), ctx.Err())
		case broadcaster.messages <- messageWire:
		}
	})

	return nil
}

// AcceptBroadcast from a remote client and propagate it to all peers in the
// network.
func (broadcaster *broadcaster) AcceptBroadcast(ctx context.Context, from protocol.PeerID, message protocol.Message) error {
	// Pre-condition checks
	if message.Version != protocol.V1 {
		return protocol.NewErrMessageVersionIsNotSupported(message.Version)
	}
	if message.Variant != protocol.Broadcast {
		return protocol.NewErrMessageVariantIsNotSupported(message.Variant)
	}

	// Ignore messages that have already been seen
	messageHash := message.Hash()
	ok, err := broadcaster.messageHashAlreadySeen(messageHash)
	if err != nil {
		return newErrBroadcastInternal(fmt.Errorf("error getting message hash=%v: %v", messageHash, err))
	}
	if ok {
		return nil
	}

	// Emit an event for this newly seen message
	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
		From:    from,
	}

	// Check if context is already expired
	select {
	case <-ctx.Done():
		return newErrAcceptingBroadcast(ctx.Err())
	default:
	}

	select {
	case <-ctx.Done():
		return newErrAcceptingBroadcast(ctx.Err())
	case broadcaster.events <- event:
	}

	// Re-broadcasting the message will downgrade its version to the version
	// supported by this broadcaster
	return broadcaster.Broadcast(ctx, message.GroupID, message.Body)
}

func (broadcaster *broadcaster) messageHashAlreadySeen(hash id.Hash) (bool, error) {
	var exists bool
	err := broadcaster.store.Get(hash.String(), &exists)
	if err == kv.ErrKeyNotFound {
		return false, nil
	}
	return exists, err
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

// ErrBroadcasting is returned when there is an error when broadcasting.
type ErrBroadcasting struct {
	error
}

func newErrBroadcasting(err error, groupID protocol.PeerGroupID) error {
	return ErrBroadcasting{
		error: fmt.Errorf("error broadcasting to group [%v] : %v", groupID, err),
	}
}

// ErrAcceptingBroadcast is returned when there is an error when accepting a
// broadcast.
type ErrAcceptingBroadcast struct {
	error
}

func newErrAcceptingBroadcast(err error) error {
	return ErrAcceptingBroadcast{
		error: fmt.Errorf("error accepting broadcast: %v", err),
	}
}
