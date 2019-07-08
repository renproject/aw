package cast

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
)

type Caster interface {
	Cast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error
	AcceptCast(ctx context.Context, to protocol.PeerID, message protocol.Message) error
}

type caster struct {
	dht      dht.DHT
	messages protocol.MessageSender
	events   protocol.EventSender
}

func NewCaster(dht dht.DHT, messages protocol.MessageSender, events protocol.EventSender) Caster {
	return &caster{
		dht:      dht,
		messages: messages,
		events:   events,
	}
}

func (caster *caster) Cast(ctx context.Context, to protocol.PeerID, body protocol.MessageBody) error {
	peerAddr, err := caster.dht.PeerAddress(to)
	if err != nil {
		return newErrCastingMessage(to, err)
	}
	if peerAddr == nil {
		return newErrCastingMessage(to, fmt.Errorf("nil peer address"))
	}

	messageWire := protocol.MessageOnTheWire{
		To:      peerAddr.NetworkAddress(),
		Message: protocol.NewMessage(protocol.V1, protocol.Cast, body),
	}
	select {
	case <-ctx.Done():
		return newErrCastingMessage(to, ctx.Err())
	case caster.messages <- messageWire:
		return nil
	}
}

func (caster *caster) AcceptCast(ctx context.Context, to protocol.PeerID, message protocol.Message) error {
	// TODO: Check for compatible message version.

	if !to.Equal(caster.dht.Me().PeerID()) {
		return newErrCastingMessage(to, fmt.Errorf("no peer available for forwarding"))
	}

	event := protocol.EventMessageReceived{
		Time:    time.Now(),
		Message: message.Body,
	}
	select {
	case <-ctx.Done():
		return newErrCastingMessage(to, ctx.Err())
	case caster.events <- event:
		return nil
	}
}

type ErrCastingMessage struct {
	error
	PeerID protocol.PeerID
}

func newErrCastingMessage(peerID protocol.PeerID, err error) error {
	return ErrCastingMessage{
		error:  fmt.Errorf("error casting to peer=%v: %v", peerID, err),
		PeerID: peerID,
	}
}
