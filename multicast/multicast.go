package multicast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Multicaster interface {
	Multicast(ctx context.Context, body protocol.MessageBody) error
	AcceptMulticast(ctx context.Context, message protocol.Message) error
}

type multicaster struct {
	dht      dht.DHT
	logger   logrus.FieldLogger
	messages protocol.MessageSender
	events   protocol.EventSender
}

func NewMulticaster(dht dht.DHT, messages protocol.MessageSender, events protocol.EventSender, logger logrus.FieldLogger) Multicaster {
	return &multicaster{
		dht:      dht,
		messages: messages,
		events:   events,
		logger:   logger,
	}
}

func (multicaster *multicaster) Multicast(ctx context.Context, body protocol.MessageBody) error {
	peerAddrs, err := multicaster.dht.PeerAddresses()
	if err != nil {
		return newErrMulticastingMessage(err)
	}
	if peerAddrs == nil {
		return newErrMulticastingMessage(fmt.Errorf("nil peer addresses"))
	}
	if len(peerAddrs) <= 0 {
		return newErrMulticastingMessage(fmt.Errorf("empty peer addresses"))
	}

	// Using the messaging sending channel protects the multicaster from
	// cascading time outs, but will still capture back pressure
	for i := range peerAddrs {
		messageWire := protocol.MessageOnTheWire{
			To:      peerAddrs[i].NetworkAddress(),
			Message: protocol.NewMessage(protocol.V1, protocol.Multicast, body),
		}
		select {
		case <-ctx.Done():
			err = newErrMulticastingMessage(ctx.Err())
		case multicaster.messages <- messageWire:
		}
	}

	// Return the last error
	return err
}

func (multicaster *multicaster) AcceptMulticast(ctx context.Context, message protocol.Message) error {
	// TODO: Check for compatible message version.

	// TODO: Multicasting will always emit an event for a received message, even
	// if the message has been seen before. Should this be changed?

	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
		From:    message.From,
	}
	select {
	case <-ctx.Done():
		return newErrMulticastingMessage(ctx.Err())
	case multicaster.events <- event:
		return nil
	}
}

type ErrMulticastingMessage struct {
	error
}

func newErrMulticastingMessage(err error) error {
	return ErrMulticastingMessage{
		error: fmt.Errorf("error multicasting: %v", err),
	}
}
