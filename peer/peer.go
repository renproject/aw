package peer

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/broadcast"
	"github.com/renproject/aw/cast"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/multicast"
	"github.com/renproject/aw/pingpong"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/kv"
	"github.com/sirupsen/logrus"
)

type Peer interface {
	dht.DHT

	Run(context.Context)

	Cast(context.Context, protocol.PeerID, protocol.MessageBody) error

	Multicast(context.Context, protocol.PeerGroupID, protocol.MessageBody) error

	Broadcast(context.Context, protocol.PeerGroupID, protocol.MessageBody) error
}

type peer struct {
	// General
	logger     logrus.FieldLogger
	codec      protocol.PeerAddressCodec
	options    Options
	dht        dht.DHT
	handshaker handshake.Handshaker
	events     protocol.EventSender

	// network connections
	client         protocol.Client
	clientMessages chan protocol.MessageOnTheWire
	server         protocol.Server
	serverMessages chan protocol.MessageOnTheWire

	// messengers
	caster      cast.Caster
	pingPonger  pingpong.PingPonger
	multicaster multicast.Multicaster
	broadcaster broadcast.Broadcaster
}

func New(logger logrus.FieldLogger, codec protocol.PeerAddressCodec, options Options, dht dht.DHT, handshaker handshake.Handshaker, client protocol.Client, server protocol.Server, events protocol.EventSender) Peer {
	if err := options.SetZeroToDefault(); err != nil {
		panic(fmt.Errorf("pre-condition violation: invalid peer option, err = %v", err))
	}

	serverMessages := make(chan protocol.MessageOnTheWire, options.Capacity)
	clientMessages := make(chan protocol.MessageOnTheWire, options.Capacity)

	pingpongOption := pingpong.Options{
		Logger:     logger,
		NumWorkers: options.NumWorkers,
		Alpha:      options.Alpha,
	}
	caster := cast.NewCaster(logger, clientMessages, events, dht)
	pingponger := pingpong.NewPingPonger(pingpongOption, dht, clientMessages, events, codec)
	multicaster := multicast.NewMulticaster(logger, options.NumWorkers, clientMessages, events, dht)
	broadcaster := broadcast.NewBroadcaster(logger, options.NumWorkers, clientMessages, events, dht)

	return &peer{
		options:    options,
		dht:        dht,
		handshaker: handshaker,
		events:     events,

		client:         client,
		clientMessages: clientMessages,
		server:         server,
		serverMessages: serverMessages,

		caster:      caster,
		pingPonger:  pingponger,
		multicaster: multicaster,
		broadcaster: broadcaster,
	}
}

func (peer *peer) Run(ctx context.Context) {
	// Start both the client and server before bootstrapping
	go peer.client.Run(ctx, peer.clientMessages)
	go peer.server.Run(ctx, peer.serverMessages)
	go peer.handleMessage(ctx)

	// Start bootstrapping
	peer.bootstrap(ctx)
	ticker := time.NewTicker(peer.options.BootstrapDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			peer.bootstrap(ctx)
		}
	}
}

func (peer *peer) Me() protocol.PeerAddress {
	return peer.dht.Me()
}

func (peer *peer) NumPeers() (int, error) {
	return peer.dht.NumPeers()
}

func (peer *peer) PeerAddress(id protocol.PeerID) (protocol.PeerAddress, error) {
	return peer.dht.PeerAddress(id)
}

func (peer *peer) PeerAddresses() (protocol.PeerAddresses, error) {
	return peer.dht.PeerAddresses()
}

func (peer *peer) RandomPeerAddresses(id protocol.PeerGroupID, n int) (protocol.PeerAddresses, error) {
	return peer.dht.RandomPeerAddresses(id, n)
}

func (peer *peer) AddPeerAddress(addrs protocol.PeerAddress) error {
	return peer.dht.AddPeerAddress(addrs)
}

func (peer *peer) UpdatePeerAddress(addr protocol.PeerAddress) (bool, error) {
	return peer.dht.UpdatePeerAddress(addr)
}

func (peer *peer) RemovePeerAddress(id protocol.PeerID) error {
	return peer.dht.RemovePeerAddress(id)
}

func (peer *peer) AddPeerGroup(groupID protocol.PeerGroupID, ids protocol.PeerIDs) error {
	return peer.dht.AddPeerGroup(groupID, ids)
}

func (peer *peer) PeerGroupIDs(groupID protocol.PeerGroupID) (protocol.PeerIDs, error) {
	return peer.PeerGroupIDs(groupID)
}

func (peer *peer) PeerGroupAddresses(groupID protocol.PeerGroupID) (protocol.PeerAddresses, error) {
	return peer.PeerGroupAddresses(groupID)
}

func (peer *peer) RemovePeerGroup(groupID protocol.PeerGroupID) {
	peer.dht.RemovePeerGroup(groupID)
}

func (peer *peer) Cast(ctx context.Context, to protocol.PeerID, data protocol.MessageBody) error {
	return peer.caster.Cast(ctx, to, data)
}

func (peer *peer) Multicast(ctx context.Context, groupID protocol.PeerGroupID, data protocol.MessageBody) error {
	return peer.multicaster.Multicast(ctx, groupID, data)
}

func (peer *peer) Broadcast(ctx context.Context, groupID protocol.PeerGroupID, data protocol.MessageBody) error {
	return peer.broadcaster.Broadcast(ctx, groupID, data)
}

func (peer *peer) bootstrap(ctx context.Context) {
	if peer.options.DisablePeerDiscovery {
		return
	}

	peerAddrs, err := peer.dht.PeerAddresses()
	if err != nil {
		peer.logger.Errorf("error bootstrapping: error loading peer addresses: %v", err)
		return
	}

	protocol.ParForAllAddresses(peerAddrs, peer.options.NumWorkers, func(peerAddr protocol.PeerAddress) {
		// Timeout is computed to ensure that we are ready for the next
		// bootstrap tick even if every single ping takes the maximum amount of
		// time (with a minimum timeout of 1 second)
		pingTimeout := time.Duration(int64(peer.options.NumWorkers) * int64(peer.options.BootstrapDuration) / int64(len(peerAddrs)))
		if pingTimeout > peer.options.BootstrapDuration {
			pingTimeout = peer.options.BootstrapDuration
		}
		if pingTimeout > peer.options.MaxPingTimeout {
			pingTimeout = peer.options.MaxPingTimeout
		}
		if pingTimeout < peer.options.MinPingTimeout {
			pingTimeout = peer.options.MinPingTimeout
		}

		pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
		defer pingCancel()
		if err := peer.pingPonger.Ping(pingCtx, peerAddr.PeerID()); err != nil {
			peer.logger.Errorf("error bootstrapping: error ping/ponging peer address=%v: %v", peerAddr, err)
			return
		}
	})
}

func (peer *peer) handleMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageOtw := <-peer.serverMessages:
			if err := peer.receiveMessageOnTheWire(ctx, messageOtw); err != nil {
				peer.logger.Error(err)
			}
		}
	}
}

func (peer *peer) receiveMessageOnTheWire(ctx context.Context, messageOtw protocol.MessageOnTheWire) error {
	switch messageOtw.Message.Variant {
	case protocol.Ping:
		return peer.pingPonger.AcceptPing(ctx, messageOtw.Message)
	case protocol.Pong:
		return peer.pingPonger.AcceptPong(ctx, messageOtw.Message)
	case protocol.Broadcast:
		return peer.broadcaster.AcceptBroadcast(ctx, messageOtw.From, messageOtw.Message)
	case protocol.Multicast:
		return peer.multicaster.AcceptMulticast(ctx, messageOtw.From, messageOtw.Message)
	case protocol.Cast:
		return peer.caster.AcceptCast(ctx, messageOtw.From, messageOtw.Message)
	default:
		return protocol.NewErrMessageVariantIsNotSupported(messageOtw.Message.Variant)
	}
}

func NewTCP(logger logrus.FieldLogger, codec protocol.PeerAddressCodec, options Options, events protocol.EventSender, signVerifier protocol.SignVerifier, poolOptions tcp.ConnPoolOptions, serverOptions tcp.ServerOptions) Peer {
	if err := options.SetZeroToDefault(); err != nil {
		panic(fmt.Errorf("pre-condition violation: invalid peer option, err = %v", err))
	}
	store := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht")
	dht, err := dht.New(options.Me, codec, store, options.BootstrapAddresses...)
	if err != nil {
		panic(fmt.Errorf("pre-condition violation: fail to initialize dht, err = %v", err))
	}
	handshaker := handshake.New(signVerifier, handshake.NewGCMSessionManager())
	connPool := tcp.NewConnPool(logger, poolOptions, handshaker)
	client := tcp.NewClient(logger, connPool)
	server := tcp.NewServer(logger, serverOptions, handshaker)
	return New(logger, codec, options, dht, handshaker, client, server, events)
}
