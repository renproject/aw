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
		return newErrMulticasting(ctx.Err())
	case multicaster.messages <- messageWire:
	}
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
	select {
	case <-ctx.Done():
		return newErrAcceptingMulticast(ctx.Err())
	case multicaster.events <- event:
		return nil
	}
}

type ErrMulticasting struct {
	error
}

func newErrMulticasting(err error) error {
	return ErrMulticasting{
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
