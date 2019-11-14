package multicast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/phi"
	"github.com/sirupsen/logrus"
)

type Multicaster interface {
	Multicast(ctx context.Context, groupID protocol.PeerGroupID, body protocol.MessageBody) error
	AcceptMulticast(ctx context.Context, message protocol.Message) error
}

type multicaster struct {
	logger   logrus.FieldLogger
	messages protocol.MessageSender
	events   protocol.EventSender
	dht      dht.DHT
}

func NewMulticaster(logger logrus.FieldLogger, messages protocol.MessageSender, events protocol.EventSender, dht dht.DHT) Multicaster {
	return &multicaster{
		logger:   logger,
		messages: messages,
		events:   events,
		dht:      dht,
	}
}

func (multicaster *multicaster) Multicast(ctx context.Context, groupID protocol.PeerGroupID, body protocol.MessageBody) error {
	ids, addrs, err := multicaster.dht.PeerGroup(groupID)
	if err != nil {
		return err
	}

	// Check if context is already expired
	select {
	case <-ctx.Done():
		return newErrMulticasting(ctx.Err(), groupID)
	default:
	}

	phi.ParForAll(addrs, func(i int) {
		to := addrs[i]
		if to == nil {
			multicaster.logger.Debugf("cannot multicast to node [%v] in group [%v], cannot find PeerAddress from dht.", ids[i], groupID)
			return
		}
		messageWire := protocol.MessageOnTheWire{
			Context: ctx,
			To:      to,
			From:    multicaster.dht.Me(),
			Message: protocol.NewMessage(protocol.V1, protocol.Multicast, groupID, body),
		}

		select {
		case <-ctx.Done():
			multicaster.logger.Debugf("cannot send message to %v, %v", to.PeerID(), ctx.Err())
		case multicaster.messages <- messageWire:
		}
	})
	return nil
}

func (multicaster *multicaster) AcceptMulticast(ctx context.Context, message protocol.Message) error {
	// TODO: Multicasting will always emit an event for a received message, even
	// if the message has been seen before. Should this be changed?
	if message.Version != protocol.V1 {
		return protocol.NewErrMessageVersionIsNotSupported(message.Version)
	}
	if message.Variant != protocol.Multicast {
		return protocol.NewErrMessageVariantIsNotSupported(message.Variant)
	}

	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}

	// Check if context is already expired
	select {
	case <-ctx.Done():
		return newErrAcceptingMulticast(ctx.Err())
	default:
	}

	select {
	case <-ctx.Done():
		return newErrAcceptingMulticast(ctx.Err())
	case multicaster.events <- event:
		return nil
	}
}

type ErrMulticasting struct {
	protocol.PeerGroupID
	error
}

func newErrMulticasting(err error, groupID protocol.PeerGroupID) error {
	return ErrMulticasting{
		PeerGroupID: groupID,
		error:       fmt.Errorf("error multicasting to group %v: %v", groupID, err),
	}
}

type ErrAcceptingMulticast struct {
	error
}

func newErrAcceptingMulticast(err error) error {
	return ErrAcceptingMulticast{
		error: fmt.Errorf("error accepting multicast: %v", err),
	}
}
