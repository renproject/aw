package peer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func defaultOptions(logger *zap.Logger) peer.Options2 {
	listenerRateLimiterOptions := peer.RateLimiterOptions{
		Rate:     1,
		Burst:    10,
		Capacity: 10,
	}
	connectionRateLimiterOptions := peer.RateLimiterOptions{
		Rate:     1 * 1024 * 1024,
		Burst:    1 * 1024 * 1024,
		Capacity: 10,
	}

	return peer.Options2{
		Logger: logger,

		MaxLinkedPeers:               2,
		MaxEphemeralConnections:      2,
		MaxPendingSyncs:              1,
		MaxActiveSyncsForSameContent: 1,
		MaxGossipSubnets:             10,
		OutgoingBufferSize:           10,
		EventLoopBufferSize:          10,
		DialRetryInterval:            time.Second,
		EphemeralConnectionTTL:       time.Second,
		MinimumConnectionExpiryAge:   time.Second,

		GossipAlpha:   2,
		GossipTimeout: time.Second,

		PingAlpha:             1,
		PongAlpha:             1,
		PeerDiscoveryInterval: time.Second,

		ListenerRateLimiterOptions:   listenerRateLimiterOptions,
		ConnectionRateLimiterOptions: connectionRateLimiterOptions,
	}
}

var _ = FDescribe("Peer2", func() {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.DebugLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	It("linked peers should successfully gossip", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := defaultOptions(logger)

		peer1 := newPeerAndListen(ctx, opts)
		peer2 := newPeerAndListen(ctx, opts)

		connectAllPeers([]*peer.Peer2{peer1, peer2})

		go peer1.Run(ctx)
		go peer2.Run(ctx)

		contentID := []byte("id")
		data := []byte("hello")
		peer1.ContentResolver.InsertContent(contentID, data)

		peer1.Link(peer2.Self)
		peer2.Link(peer1.Self)

		peer1.Gossip(contentID, nil)

		Eventually(func() []byte { msg, _ := peer2.ContentResolver.QueryContent(contentID); return msg }).Should(Equal(data))
	})

	It("should gossip a message to everyone in a ring topology", func() {
		n := 10

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := defaultOptions(logger)

		assert(opts.GossipAlpha >= 2)

		peers := make([]*peer.Peer2, n)
		for i := range peers {
			peers[i] = newPeerAndListen(ctx, opts)
		}

		connectPeersRing(peers)

		for _, peer := range peers {
			go peer.Run(ctx)
		}

		linkPeersRing(peers)

		contentID := []byte("id")
		data := []byte("hello")
		peers[0].ContentResolver.InsertContent(contentID, data)

		peers[0].Gossip(contentID, nil)

		for _, peer := range peers {
			Eventually(func() []byte { msg, _ := peer.ContentResolver.QueryContent(contentID); return msg }).Should(Equal(data))
		}
	})
})

func newPeerAndListen(ctx context.Context, opts peer.Options2) *peer.Peer2 {
	privKey := id.NewPrivKey()
	peerTable := dht.NewInMemTable(privKey.Signatory())
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	peer := peer.New2(opts, privKey, peerTable, contentResolver)

	_, err := peer.Listen(ctx, "localhost:0")
	if err != nil {
		panic(err)
	}

	return peer
}

func connectAllPeers(peers []*peer.Peer2) {
	addresses := make([]wire.Address, len(peers))
	for i := range peers {
		addresses[i] = wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", peers[i].Port), uint64(time.Now().UnixNano()))
	}

	for i := range peers {
		for j := range peers {
			if j != i {
				peers[i].PeerTable.AddPeer(peers[j].Self, addresses[j])
				peers[j].PeerTable.AddPeer(peers[i].Self, addresses[i])
			}
		}
	}
}

func connectPeersRing(peers []*peer.Peer2) {
	n := len(peers)

	addresses := make([]wire.Address, n)
	for i := range peers {
		addresses[i] = wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", peers[i].Port), uint64(time.Now().UnixNano()))
	}

	for i := range peers {
		next := (i + 1) % n
		peers[i].PeerTable.AddPeer(peers[next].Self, addresses[next])
		peers[next].PeerTable.AddPeer(peers[i].Self, addresses[i])
	}
}

func linkPeersRing(peers []*peer.Peer2) {
	n := len(peers)

	for i := range peers {
		next := (i + 1) % n
		peers[i].Link(peers[next].Self)
		peers[next].Link(peers[i].Self)
	}
}

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
