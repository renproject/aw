package cast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Caster interface {
	Cast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error
	AcceptCast(ctx context.Context, message protocol.Message) error
}

type caster struct {
	logger   logrus.FieldLogger
	messages protocol.MessageSender
	events   protocol.EventSender
	dht      dht.DHT
}

func NewCaster(logger logrus.FieldLogger, messages protocol.MessageSender, events protocol.EventSender, dht dht.DHT) Caster {
	return &caster{
		logger:   logger,
		messages: messages,
		events:   events,
		dht:      dht,
	}
}

func (caster *caster) Cast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error {
	toAddr, err := caster.dht.PeerAddress(to)
	if err != nil {
		return err
	}
	message := protocol.MessageOnTheWire{
		Context: ctx,
		To:      toAddr,
		From:    caster.dht.Me(),
		Message: protocol.NewMessage(protocol.V1, protocol.Cast, body),
	}

	// Check if context is already expired
	select {
	case <-ctx.Done():
		return newErrCasting(to, ctx.Err())
	default:
	}

	// Try to send the message via the message sender within the given context.
	select {
	case <-ctx.Done():
		return newErrCasting(to, ctx.Err())
	case caster.messages <- message:
		return nil
	}
}

func (caster *caster) AcceptCast(ctx context.Context, message protocol.Message) error {
	// TODO: Update to allow message forwarding.
	// Pre-condition checks
	if message.Version != protocol.V1 {
		return protocol.NewErrMessageVersionIsNotSupported(message.Version)
	}
	if message.Variant != protocol.Cast {
		return protocol.NewErrMessageVariantIsNotSupported(message.Variant)
	}

	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}

	// Check if context is already expired
	select {
	case <-ctx.Done():
		return fmt.Errorf("error accepting cast: %v", ctx.Err())
	default:
	}

	// Try to send the event via the event sender within the given context.
	select {
	case <-ctx.Done():
		return fmt.Errorf("error accepting cast: %v", ctx.Err())
	case caster.events <- event:
		return nil
	}
}

type ErrCasting struct {
	error
	PeerID protocol.PeerID
}

func newErrCasting(peerID protocol.PeerID, err error) error {
	return ErrCasting{
		error:  fmt.Errorf("error casting to %v: %v", peerID, err),
		PeerID: peerID,
	}
}
