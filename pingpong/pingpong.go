package pingpong

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Options struct {
	Logger     logrus.FieldLogger
	NumWorkers int
	Alpha      int
}

type PingPonger interface {
	Ping(ctx context.Context, to protocol.PeerID) error
	AcceptPing(ctx context.Context, message protocol.Message) error
	AcceptPong(ctx context.Context, message protocol.Message) error
}

type pingPonger struct {
	options  Options
	dht      dht.DHT
	messages protocol.MessageSender
	events   protocol.EventSender
	codec    protocol.PeerAddressCodec
}

func NewPingPonger(options Options, dht dht.DHT, messages protocol.MessageSender, events protocol.EventSender, codec protocol.PeerAddressCodec) PingPonger {
	return &pingPonger{
		options:  options,
		dht:      dht,
		messages: messages,
		events:   events,
		codec:    codec,
	}
}

func (pp *pingPonger) Ping(ctx context.Context, to protocol.PeerID) error {
	peerAddr, err := pp.dht.PeerAddress(to)
	if err != nil {
		return err
	}

	me, err := pp.codec.Encode(pp.dht.Me())
	if err != nil {
		return err
	}
	messageWire := protocol.MessageOnTheWire{
		To:      peerAddr,
		Message: protocol.NewMessage(protocol.V1, protocol.Ping, protocol.NilGroupID, me),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case pp.messages <- messageWire:
		return nil
	}
}

func (pp *pingPonger) AcceptPing(ctx context.Context, message protocol.Message) error {
	// Pre-condition checks
	if message.Version != protocol.V1 {
		return protocol.NewErrMessageVersionIsNotSupported(message.Version)
	}
	if message.Variant != protocol.Ping {
		return protocol.NewErrMessageVariantIsNotSupported(message.Variant)
	}

	peerAddr, err := pp.codec.Decode(message.Body)
	if err != nil {
		return newErrDecodingMessage(err, protocol.Ping, message.Body)
	}

	// if the peer address contains this peer's address do not add it to the DHT,
	// and stop propagating the message to other peers.
	if peerAddr.PeerID().Equal(pp.dht.Me().PeerID()) {
		return nil
	}

	didUpdate, err := pp.updatePeerAddress(ctx, peerAddr)
	if err != nil || !didUpdate {
		return err
	}

	// todo : should this be put inside a goroutine.
	if err := pp.pong(ctx, peerAddr); err != nil {
		return err
	}

	// Propagating the ping will downgrade the ping to the version of this
	// pinger/ponger
	return pp.propagatePing(ctx, peerAddr.PeerID(), message.Body)
}

func (pp *pingPonger) AcceptPong(ctx context.Context, message protocol.Message) error {
	// Pre-condition checks
	if message.Version != protocol.V1 {
		return protocol.NewErrMessageVersionIsNotSupported(message.Version)
	}
	if message.Variant != protocol.Pong {
		return protocol.NewErrMessageVariantIsNotSupported(message.Variant)
	}

	peerAddr, err := pp.codec.Decode(message.Body)
	if err != nil {
		return newErrDecodingMessage(err, protocol.Pong, message.Body)
	}
	_, err = pp.updatePeerAddress(ctx, peerAddr)
	return err
}

func (pp *pingPonger) pong(ctx context.Context, to protocol.PeerAddress) error {
	me, err := pp.codec.Encode(pp.dht.Me())
	if err != nil {
		return err
	}
	messageWire := protocol.MessageOnTheWire{
		To:      to,
		Message: protocol.NewMessage(protocol.V1, protocol.Pong, protocol.NilGroupID, me),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pp.messages <- messageWire:
		return nil
	}
}

func (pp *pingPonger) propagatePing(ctx context.Context, sender protocol.PeerID, body protocol.MessageBody) error {
	peerAddrs, err := pp.dht.RandomPeerAddresses(protocol.NilGroupID, pp.options.Alpha)
	if err != nil {
		return err
	}

	protocol.ParForAllAddresses(peerAddrs, pp.options.NumWorkers, func(addr protocol.PeerAddress) {
		if addr.PeerID().Equal(sender) {
			return
		}
		messageWire := protocol.MessageOnTheWire{
			To:      addr,
			Message: protocol.NewMessage(protocol.V1, protocol.Ping, protocol.NilGroupID, body),
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case pp.messages <- messageWire:
		}
	})

	return nil
}

func (pp *pingPonger) updatePeerAddress(ctx context.Context, peerAddr protocol.PeerAddress) (bool, error) {
	updated, err := pp.dht.UpdatePeerAddress(peerAddr)
	if err != nil || !updated {
		return updated, err
	}

	event := protocol.EventPeerChanged{
		Time:        time.Now(),
		PeerAddress: peerAddr,
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case pp.events <- event:
		return true, nil
	}
}

func newErrDecodingMessage(err error, variant protocol.MessageVariant, message []byte) error {
	return fmt.Errorf("cannot decode %v message [%v], err = %v", variant, base64.RawStdEncoding.EncodeToString(message), err)
}
