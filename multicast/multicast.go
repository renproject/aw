package multicast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Multicaster interface {
	Multicast(ctx context.Context, body protocol.MessageBody) error
	AcceptMulticast(ctx context.Context, message protocol.Message) error
}

type multicaster struct {
	logger   logrus.FieldLogger
	messages protocol.MessageSender
	events   protocol.EventSender
}

func NewMulticaster(messages protocol.MessageSender, events protocol.EventSender, logger logrus.FieldLogger) Multicaster {
	return &multicaster{
		messages: messages,
		events:   events,
		logger:   logger,
	}
}

func (multicaster *multicaster) Multicast(ctx context.Context, body protocol.MessageBody) error {
	messageWire := protocol.MessageOnTheWire{
		Message: protocol.NewMessage(protocol.V1, protocol.Multicast, body),
	}
	select {
	case <-ctx.Done():
		return newErrMulticastingMessage(fmt.Errorf("error sending: %v", ctx.Err()))
	case multicaster.messages <- messageWire:
	}
	return nil
}

func (multicaster *multicaster) AcceptMulticast(ctx context.Context, message protocol.Message) error {
	// TODO: Multicasting will always emit an event for a received message, even
	// if the message has been seen before. Should this be changed?
	if message.Version != protocol.V1 {
		return newErrMulticastVersionNotSupported(message.Version)
	}
	if message.Variant != protocol.Multicast {
		return newErrMulticastVariantNotSupported(message.Variant)
	}

	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}
	select {
	case <-ctx.Done():
		return newErrAcceptingMulticast(ctx.Err())
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

type ErrAcceptingMulticast struct {
	error
}

func newErrAcceptingMulticast(err error) error {
	return ErrAcceptingMulticast{
		error: fmt.Errorf("error accepting multicast: %v", err),
	}
}

// ErrMulticastVersionNotSupported is returned when a multicast message has an
// unsupported version.
type ErrMulticastVersionNotSupported struct {
	error
}

func newErrMulticastVersionNotSupported(version protocol.MessageVersion) error {
	return ErrMulticastVersionNotSupported{
		error: fmt.Errorf("multicast version=%v not supported", version),
	}
}

// ErrMulticastVariantNotSupported is returned when a multicast message has an
// unsupported variant.
type ErrMulticastVariantNotSupported struct {
	error
}

func newErrMulticastVariantNotSupported(variant protocol.MessageVariant) error {
	return ErrMulticastVariantNotSupported{
		error: fmt.Errorf("multicast variant=%v not supported", variant),
	}
}
