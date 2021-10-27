package aw_test

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*
 * TODO(ross): Tests to write:
 *     - Fuzzing/malicious message data tests
 *     - Sync tests
 *     - Handshake tests?
 *     - Keep alive/unique connection tests?
 *     - Sending a message before a connection is established still send the message
 *     - Changing the underlying connection doesn't drop messages (and will in fact be in the same order)
 *
 * Replace these exmaple programs?
 *     - Peers in a ring topology, all gossip a unique message over many
 *       rounds; track throughput and make sure all messages are received
 *     - Existing peers that know about eachother, and then a new peer that the
 *       existing peers don't know about joins and is eventually discovered by
 *       the others too
 *     - Many rounds of syncing a message from a peer (hint is given) and
 *       success and throughput is checked
 *     - Peers sending messages to eachother but randomly crashing/restarting
 */

func defaultOptions(logger *zap.Logger) aw.Options {
	listenerOptions := aw.ListenerOptions{
		RateLimiterCapacity: 10,
		RateLimiterOptions: aw.RateLimiterOptions{
			Rate:  1,
			Burst: 10,
		},
	}
	connectionRateLimiterOptions := aw.RateLimiterOptions{
		Rate:  1 * 1024 * 1024,
		Burst: 1 * 1024 * 1024,
	}

	return aw.Options{
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
		PeerExpiryTimeout:     time.Second,

		ListenerOptions:              listenerOptions,
		ConnectionRateLimiterOptions: connectionRateLimiterOptions,
	}
}

var _ = Describe("Peer", func() {
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

		connectAllPeers([]*aw.Peer{peer1, peer2})

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

	It("unlinked peers should successfully gossip", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := defaultOptions(logger)

		peer1 := newPeerAndListen(ctx, opts)
		peer2 := newPeerAndListen(ctx, opts)

		connectAllPeers([]*aw.Peer{peer1, peer2})

		go peer1.Run(ctx)
		go peer2.Run(ctx)

		contentID := []byte("id")
		data := []byte("hello")
		peer1.ContentResolver.InsertContent(contentID, data)

		peer1.Gossip(contentID, nil)

		Eventually(func() []byte { msg, _ := peer2.ContentResolver.QueryContent(contentID); return msg }).Should(Equal(data))
	})

	It("should gossip a message to everyone in a ring topology", func() {
		n := 10

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := defaultOptions(logger)

		assert(opts.GossipAlpha >= 2)

		peers := make([]*aw.Peer, n)
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

	It("peers should discover eachother in a ring topology", func() {
		n := 10

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := defaultOptions(logger)
		opts.PeerDiscoveryInterval = 10 * time.Millisecond
		opts.PingAlpha = 5
		opts.PongAlpha = 5

		peers := make([]*aw.Peer, n)
		for i := range peers {
			peers[i] = newPeerAndListen(ctx, opts)
		}

		connectPeersRing(peers)

		for _, peer := range peers {
			go peer.Run(ctx)
		}

		peerIDs := make([]id.Signatory, len(peers))
		for i := range peers {
			peerIDs[i] = peers[i].Self
		}

		hasDiscoveredAllOthers := func(peer *aw.Peer, ids []id.Signatory) bool {
			for _, other := range ids {
				if !other.Equal(&peer.Self) {
					if _, ok := peer.PeerTable.PeerAddress(other); !ok {
						return false
					}
				}
			}

			return true
		}

		for _, peer := range peers {
			Eventually(func() bool { return hasDiscoveredAllOthers(peer, peerIDs) }, 100).Should(BeTrue())
		}
	})
})

func newPeerAndListen(ctx context.Context, opts aw.Options) *aw.Peer {
	privKey := id.NewPrivKey()
	peerTable := dht.NewInMemTable(privKey.Signatory())
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	peer := aw.New(opts, privKey, peerTable, contentResolver)

	_, err := peer.Listen(ctx, "localhost:0")
	if err != nil {
		panic(err)
	}

	return peer
}

func connectAllPeers(peers []*aw.Peer) {
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

func connectPeersRing(peers []*aw.Peer) {
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

func linkPeersRing(peers []*aw.Peer) {
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
