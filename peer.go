package aw

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/renproject/aw/broadcast"
	"github.com/renproject/aw/cast"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/multicast"
	"github.com/renproject/aw/pingpong"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/kv"
	"github.com/sirupsen/logrus"
)

type PeerOptions struct {
	Me                 PeerAddress
	BootstrapAddresses PeerAddresses
	Codec              PeerAddressCodec

	// Optional
	EventsCap      int                // Defaults to unbuffered
	BootstrapDelay time.Duration      // Defaults to 1 Minute
	DHTStore       kv.Iterable        // Defaults to use in memory store
	BroadcastStore kv.Store           // Defaults to use in memory store
	Logger         logrus.FieldLogger // Defaults to discard logger
}

type Peer interface {
	Run(context.Context) error

	Peer(context.Context, PeerID) (PeerAddress, error)
	Peers(context.Context) (PeerAddresses, error)
	NumPeers(context.Context) (int, error)
}

type peer struct {
	bootstrapDelay time.Duration
	logger         logrus.FieldLogger

	broadcaster broadcast.Broadcaster
	caster      cast.Caster
	multicaster multicast.Multicaster
	pingPonger  pingpong.PingPonger
	dht         dht.DHT

	sender      MessageSender
	receiver    MessageReceiver
	eventSender EventSender
}

func New(options PeerOptions, sender MessageSender, receiver MessageReceiver) (Peer, EventReceiver) {
	events := make(chan Event, options.EventsCap)
	if options.Logger == nil {
		logger := logrus.New()
		logger.SetOutput(ioutil.Discard)
		options.Logger = logger
	}

	if options.BootstrapDelay == 0 {
		options.BootstrapDelay = time.Minute
	}

	if options.BroadcastStore == nil {
		options.BroadcastStore = kv.NewGob(kv.NewMemDB())
	}

	if options.DHTStore == nil {
		options.DHTStore = kv.NewGob(kv.NewMemDB())
	}

	// pre-condition check
	if err := validateOptions(options); err != nil {
		// FIXME: handle the error without panicing
		panic(newErrInvalidPeerOptions(err))
	}

	dht, err := dht.New(options.Me, options.Codec, options.DHTStore, options.BootstrapAddresses...)
	if err != nil {
		// FIXME: handle the error without panicing
		panic(fmt.Errorf("failed to initialize DHT: %v", err))
	}

	return &peer{
		bootstrapDelay: options.BootstrapDelay,
		logger:         options.Logger,

		broadcaster: broadcast.NewBroadcaster(broadcast.NewStorage(options.BroadcastStore), dht, sender, events),
		multicaster: multicast.NewMulticaster(dht, sender, events),
		caster:      cast.NewCaster(dht, sender, events),
		pingPonger:  pingpong.NewPingPonger(dht, sender, events, options.Codec),
		dht:         dht,

		sender:      sender,
		receiver:    receiver,
		eventSender: events,
	}, events
}

func (peer *peer) Run(ctx context.Context) error {
	ticker := time.NewTicker(peer.bootstrapDelay)
	for {
		select {
		case <-ctx.Done():
			return newErrStoppingBootstrap(ctx.Err())
		case msg := <-peer.receiver:
			go peer.handleIncommingMessage(ctx, msg)
		case <-ticker.C:
			peerAddrs, err := peer.dht.PeerAddresses()
			if err != nil {
				return err
			}
			for _, peerAddr := range peerAddrs {
				if err := peer.pingPonger.Ping(ctx, peerAddr.PeerID()); err != nil {
					return err
				}
			}
		}
	}
}

func (peer *peer) Peer(ctx context.Context, peerID PeerID) (PeerAddress, error) {
	return peer.dht.PeerAddress(peerID)
}

func (peer *peer) Peers(context.Context) (PeerAddresses, error) {
	return peer.dht.PeerAddresses()
}

func (peer *peer) NumPeers(context.Context) (int, error) {
	return peer.dht.NumPeers()
}

func (peer *peer) handleIncommingMessage(ctx context.Context, msg protocol.MessageOnTheWire) error {
	go func(ctx context.Context) {
		select {
		case <-ctx.Done():
			peer.logger.Errorf("failed to write to the events channel: %v", ctx.Err())
			return
		case peer.eventSender <- protocol.EventMessageReceived{
			Time:    time.Now(),
			Message: msg.Message.Body,
		}:
		}
	}(ctx)

	switch msg.Message.Variant {
	case protocol.Ping:
		return peer.pingPonger.AcceptPing(ctx, msg.Message)
	case protocol.Pong:
		return peer.pingPonger.AcceptPong(ctx, msg.Message)
	case protocol.Broadcast:
		return peer.broadcaster.AcceptBroadcast(ctx, msg.Message)
	case protocol.Multicast:
		return peer.multicaster.AcceptMulticast(ctx, msg.Message)
	case protocol.Cast:
		return peer.caster.AcceptCast(ctx, msg.Message)
	default:
		return fmt.Errorf("unknown variant: %d", msg.Message.Variant)
	}
}

func validateOptions(options PeerOptions) error {
	if options.Me == nil {
		return fmt.Errorf("nil me address")
	}
	if options.BootstrapAddresses == nil {
		return fmt.Errorf("no bootstrap addresses provided")
	}
	if options.Codec == nil {
		return fmt.Errorf("no peer address codec provided")
	}
	return nil
}

func newErrPeerNotFound(err error) error {
	return fmt.Errorf("peer not found: %v", err)
}

func newErrStoppingBootstrap(err error) error {
	return fmt.Errorf("stopping bootstrap: %v", err)
}

func newErrInvalidPeerOptions(err error) error {
	return fmt.Errorf("invalid peer options: %v", err)
}
