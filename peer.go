package aw

import (
	"context"
	"fmt"
	"runtime"
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
	"github.com/renproject/phi"
	"github.com/sirupsen/logrus"
)

type RunFn func(context.Context)

type PeerOptions struct {
	Logger logrus.FieldLogger

	Me                 PeerAddress
	BootstrapAddresses PeerAddresses
	Codec              PeerAddressCodec

	// Optional
	DisablePeerDiscovery bool          `json:"disablePeerDiscovery"` // Defaults to false
	EventBuffer          int           `json:"eventBuffer"`          // Defaults to 0
	BootstrapWorkers     int           `json:"bootstrapWorkers"`     // Defaults to 2x the number of CPUs
	BootstrapDuration    time.Duration `json:"bootstrapDuration"`    // Defaults to 1 hour

	DHTStore         kv.Table              // Defaults to using in memory store
	BroadcasterStore kv.Table              // Defaults to using in memory store
	SignVerifier     protocol.SignVerifier // Defaults to nil
	RunFns           []RunFn               // Defaults to nil
}

type Peer interface {
	Run(context.Context)
	Peer(context.Context, PeerID) (PeerAddress, error)
	Peers(context.Context) (PeerAddresses, error)
	AddPeer(context.Context, PeerAddress) error
	AddPeers(context.Context, PeerAddresses) error
	NumPeers(context.Context) (int, error)
	Cast(context.Context, PeerID, []byte) error
	Multicast(context.Context, []byte) error
	Broadcast(context.Context, []byte) error
}

type peer struct {
	options PeerOptions

	dht         dht.DHT
	pingPonger  pingpong.PingPonger
	caster      cast.Caster
	multicaster multicast.Multicaster
	broadcaster broadcast.Broadcaster

	receiver MessageReceiver
}

func New(options PeerOptions, receiver MessageReceiver, dht dht.DHT, pingponger pingpong.PingPonger, caster cast.Caster, multicaster multicast.Multicaster, broadcaster broadcast.Broadcaster) Peer {
	// Pre-condition check
	if err := validateOptions(options); err != nil {
		panic(fmt.Errorf("pre-condition violation: %v", newErrInvalidPeerOptions(err)))
	}

	// Set default values
	if options.BootstrapDuration <= 0 {
		options.BootstrapDuration = time.Hour
	}
	if options.BootstrapWorkers <= 0 {
		options.BootstrapWorkers = 2 * runtime.NumCPU()
	}

	return &peer{
		options: options,

		dht:         dht,
		pingPonger:  pingponger,
		caster:      caster,
		multicaster: multicaster,
		broadcaster: broadcaster,

		receiver: receiver,
	}
}

func (peer *peer) Run(ctx context.Context) {
	if peer.options.RunFns != nil {
		for _, fn := range peer.options.RunFns {
			go fn(ctx)
		}
	}

	peer.bootstrap(ctx)
	ticker := time.NewTicker(peer.options.BootstrapDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			peer.bootstrap(ctx)

		case m := <-peer.receiver:
			if err := peer.receiveMessageOnTheWire(ctx, m); err != nil {
				peer.options.Logger.Error(err)
			}
		}
	}
}

func (peer *peer) AddPeer(_ context.Context, address PeerAddress) error {
	return peer.dht.AddPeerAddress(address)
}

func (peer *peer) AddPeers(_ context.Context, addresses PeerAddresses) error {
	return peer.dht.AddPeerAddresses(addresses)
}

func (peer *peer) Peer(_ context.Context, peerID PeerID) (PeerAddress, error) {
	return peer.dht.PeerAddress(peerID)
}

func (peer *peer) Peers(_ context.Context) (PeerAddresses, error) {
	return peer.dht.PeerAddresses()
}

func (peer *peer) NumPeers(_ context.Context) (int, error) {
	return peer.dht.NumPeers()
}

func (peer *peer) Cast(ctx context.Context, to PeerID, data []byte) error {
	return peer.caster.Cast(ctx, to, protocol.MessageBody(data))
}

func (peer *peer) Multicast(ctx context.Context, data []byte) error {
	return peer.multicaster.Multicast(ctx, protocol.MessageBody(data))
}

func (peer *peer) Broadcast(ctx context.Context, data []byte) error {
	return peer.broadcaster.Broadcast(ctx, protocol.MessageBody(data))
}

func (peer *peer) bootstrap(ctx context.Context) {
	if peer.options.DisablePeerDiscovery {
		return
	}
	// Load all peer addresses into a fully buffered queue so that
	// workers can process them most efficiently
	peerAddrs, err := peer.dht.PeerAddresses()
	if err != nil {
		peer.options.Logger.Errorf("error bootstrapping: error loading peer addresses: %v", err)
		return
	}
	peerAddrsQ := make(chan PeerAddress, len(peerAddrs))
	for _, peerAddr := range peerAddrs {
		peerAddrsQ <- peerAddr
	}
	close(peerAddrsQ)

	// Spawn multiple goroutine workers to process the peer addresses in the
	// queue one-by-one
	phi.ForAll(peer.options.BootstrapWorkers, func(_ int) {
		peerAddr, ok := <-peerAddrsQ
		if !ok {
			return
		}

		// Timeout is computed to ensure that we are ready for the next
		// bootstrap tick even if every single ping takes the maximum amount of
		// time (with a minimum timeout of 1 second)
		pingTimeout := time.Duration(int64(peer.options.BootstrapWorkers) * int64(peer.options.BootstrapDuration) / int64(len(peerAddrs)))
		if pingTimeout > peer.options.BootstrapDuration {
			pingTimeout = peer.options.BootstrapDuration
		}
		if pingTimeout > 30*time.Second {
			pingTimeout = 30 * time.Second
		}
		if pingTimeout < time.Second {
			pingTimeout = time.Second
		}

		pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
		defer pingCancel()

		if err := peer.pingPonger.Ping(pingCtx, peerAddr.PeerID()); err != nil {
			peer.options.Logger.Errorf("error bootstrapping: error ping/ponging peer address=%v: %v", peerAddr, err)
			return
		}
	})
}

func (peer *peer) receiveMessageOnTheWire(ctx context.Context, messageOtw protocol.MessageOnTheWire) error {
	switch messageOtw.Message.Variant {
	case protocol.Ping:
		return peer.pingPonger.AcceptPing(ctx, messageOtw.Message)
	case protocol.Pong:
		return peer.pingPonger.AcceptPong(ctx, messageOtw.Message)
	case protocol.Broadcast:
		return peer.broadcaster.AcceptBroadcast(ctx, messageOtw.Message)
	case protocol.Multicast:
		return peer.multicaster.AcceptMulticast(ctx, messageOtw.Message)
	case protocol.Cast:
		return peer.caster.AcceptCast(ctx, messageOtw.Message)
	default:
		return fmt.Errorf("unexpected message variant=%v", messageOtw.Message.Variant)
	}
}

func validateOptions(options PeerOptions) error {
	if options.Logger == nil {
		return fmt.Errorf("nil logger")
	}
	if options.Me == nil {
		return fmt.Errorf("nil me address")
	}
	if options.Codec == nil {
		return fmt.Errorf("nil peer address codec")
	}
	return nil
}

func newErrBootstrapCanceled(err error) error {
	return fmt.Errorf("bootstrap canceled: %v", err)
}

func newErrPeerNotFound(err error) error {
	return fmt.Errorf("peer not found: %v", err)
}

func newErrInvalidPeerOptions(err error) error {
	return fmt.Errorf("invalid peer options: %v", err)
}

func DefaultTCP(options PeerOptions, events EventSender, cap, port int) Peer {
	var handshaker handshake.Handshaker
	if options.SignVerifier != nil {
		handshaker = handshake.New(options.SignVerifier)
	}
	serverMessages := make(chan protocol.MessageOnTheWire, cap)
	clientMessages := make(chan protocol.MessageOnTheWire, cap)
	options.RunFns = []RunFn{
		func(ctx context.Context) {
			err := tcp.NewServer(tcp.ServerOptions{
				Logger:     options.Logger,
				Timeout:    time.Minute,
				Handshaker: handshaker,
			}, serverMessages).Listen(ctx, fmt.Sprintf("0.0.0.0:%v", port))
			if err != nil {
				panic(fmt.Errorf("tcp server has crashed: %v", err))
			}
		},
		func(ctx context.Context) {
			tcp.NewClient(tcp.NewClientConns(tcp.ClientOptions{
				Logger:         options.Logger,
				Timeout:        10 * time.Second,
				Handshaker:     handshaker,
				MaxConnections: 200,
			}), clientMessages).Run(ctx)
		},
	}
	return Default(options, serverMessages, clientMessages, events)
}

func Default(options PeerOptions, receiver MessageReceiver, sender MessageSender, events EventSender) Peer {
	// Pre-condition check
	if err := validateOptions(options); err != nil {
		panic(fmt.Errorf("pre-condition violation: %v", newErrInvalidPeerOptions(err)))
	}

	if options.DHTStore == nil {
		options.DHTStore = kv.NewTable(kv.NewMemDB(kv.GobCodec), "dht")
	}

	if options.BroadcasterStore == nil {
		options.BroadcasterStore = kv.NewTable(kv.NewMemDB(kv.GobCodec), "broadcaster")
	}

	dht, err := dht.New(options.Me, options.Codec, options.DHTStore, options.BootstrapAddresses...)
	if err != nil {
		panic(fmt.Errorf("failed to initialize DHT: %v", err))
	}

	return New(
		options,
		receiver,
		dht,
		pingpong.NewPingPonger(dht, sender, events, options.Codec, options.Logger),
		cast.NewCaster(dht, sender, events, options.Logger),
		multicast.NewMulticaster(dht, sender, events, options.Logger),
		broadcast.NewBroadcaster(broadcast.NewStorage(options.BroadcasterStore), dht, sender, events, options.Logger),
	)
}
