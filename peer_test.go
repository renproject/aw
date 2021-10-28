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
 *     - Handshake tests?
 *     - Keep alive/unique connection tests?
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

		MaxLinkedPeers:               10,
		MaxEphemeralConnections:      10,
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
	loggerConfig.Level.SetLevel(zap.WarnLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	Context("gossiping", func() {
		Specify("linked peers should successfully gossip", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)

			contentID := []byte("id")
			content := []byte("hello")
			peers := manyConnectedPeersFirstHasContent(ctx, 2, opts, contentID, content)
			peer1 := peers[0]
			peer2 := peers[1]

			peer1.Gossip(contentID, nil)

			Eventually(func() []byte { msg, _ := peer2.ContentResolver.QueryContent(contentID); return msg }).Should(Equal(content))
		})

		Specify("unlinked peers should successfully gossip", func() {
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

		Specify("messages sent before a connection is established should arrive", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)
			opts.DialRetryInterval = 100 * time.Millisecond
			opts.PeerExpiryTimeout = time.Hour
			opts.PeerDiscoveryInterval = time.Hour
			opts.EphemeralConnectionTTL = time.Hour
			peer1 := newPeer(opts)
			peer2 := newPeer(opts)

			contentID := []byte("id")
			content := []byte("hello")
			peer1.ContentResolver.InsertContent(contentID, content)

			peer2Port := 3333
			address2 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", peer2Port), uint64(time.Now().UnixNano()))
			peer1.PeerTable.AddPeer(peer2.Self, address2)

			_, err = peer1.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}
			go peer1.Run(ctx)

			peer1.Gossip(contentID, nil)

			time.Sleep(50 * time.Millisecond)

			_, err = peer2.Listen(ctx, fmt.Sprintf("localhost:%v", peer2Port))
			if err != nil {
				panic(err)
			}
			go peer2.Run(ctx)

			Eventually(func() []byte { msg, _ := peer2.ContentResolver.QueryContent(contentID); return msg }, 1000).Should(Equal(content))
		})
	})

	Context("peer discovery", func() {
		Specify("peers should discover eachother in a ring topology", func() {
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

	Context("syncing", func() {
		It("should sync messages correctly with two peers", func() {
			// NOTE: This test sometimes causes a warning about a reset
			// connection during a handshake to be printed. This only seems to
			// occur when this test is run as part of the whole suite; I don't
			// see it when just this test is focused.
			//
			// I think what is happening is that when a context is cancelled it
			// can be sort of random when and in what order anything selecting
			// on that context will fall through and so we can have a
			// handshaking go routine still alive and trying to read from a
			// connection that is now closed. Doing such a read would cause a
			// reset connection error.
			//
			// It is curious that these errors don't occur during any other
			// tests though.

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)

			contentID := []byte("id")
			content := []byte("hello")
			peers := manyConnectedPeersFirstHasContent(ctx, 2, opts, contentID, content)

			syncCtx, cancel := context.WithTimeout(ctx, time.Second)
			receivedData, err := peers[1].Sync(syncCtx, contentID, nil)
			cancel()

			Expect(err).To(BeNil())
			Expect(receivedData).To(Equal(content))
		})

		Context("many peers and only one has the content", func() {
			It("should sync the content with no hint", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				n := 5
				opts := defaultOptions(logger)
				opts.GossipAlpha = n - 1

				contentID := []byte("id")
				content := []byte("hello")
				peers := manyConnectedPeersFirstHasContent(ctx, n, opts, contentID, content)

				for _, peer := range peers[1:] {
					syncCtx, cancel := context.WithTimeout(ctx, time.Second)
					receivedData, err := peer.Sync(syncCtx, contentID, nil)
					cancel()

					Expect(err).To(BeNil())
					Expect(receivedData).To(Equal(content))
				}
			})

			It("should sync the content with an accurate hint", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				n := 10
				opts := defaultOptions(logger)
				opts.GossipAlpha = 0 // Ensure the sync will fail if the hint is wrong.

				contentID := []byte("id")
				content := []byte("hello")
				peers := manyConnectedPeersFirstHasContent(ctx, n, opts, contentID, content)

				hint := peers[0].Self

				for _, peer := range peers[1:] {
					syncCtx, cancel := context.WithTimeout(ctx, time.Second)
					receivedData, err := peer.Sync(syncCtx, contentID, &hint)
					cancel()

					Expect(err).To(BeNil())
					Expect(receivedData).To(Equal(content))
				}
			})
		})
	})

	Context("peer expiry", func() {
		Specify("peers should be removed from the table after dialing fails after a timeout", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)
			opts.PeerExpiryTimeout = 10 * time.Millisecond
			// opts.DialRetryInterval = 10 * time.Millisecond
			opts.EphemeralConnectionTTL = 10 * time.Millisecond
			opts.PeerDiscoveryInterval = 10 * time.Millisecond

			peer := newPeerAndListen(ctx, opts)
			signatory := id.NewPrivKey().Signatory()
			address := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", 12345), uint64(time.Now().UnixNano()))
			peer.PeerTable.AddPeer(signatory, address)

			Expect(peer.PeerTable.NumPeers()).To(Equal(1))

			go peer.Run(ctx)

			Eventually(peer.PeerTable.NumPeers).Should(Equal(0))
		})
	})
})

func manyConnectedPeersFirstHasContent(ctx context.Context, n int, opts aw.Options, contentID, content []byte) []*aw.Peer {
	peers := make([]*aw.Peer, n)
	for i := range peers {
		peers[i] = newPeerAndListen(ctx, opts)
	}

	connectAllPeers(peers)

	for _, peer := range peers {
		go peer.Run(ctx)
	}

	linkAllPeers(peers)

	peers[0].ContentResolver.InsertContent(contentID, content)

	return peers
}

func newPeer(opts aw.Options) *aw.Peer {
	privKey := id.NewPrivKey()
	peerTable := dht.NewInMemTable(privKey.Signatory())
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	peer := aw.New(opts, privKey, peerTable, contentResolver)

	return peer
}

func newPeerAndListen(ctx context.Context, opts aw.Options) *aw.Peer {
	peer := newPeer(opts)

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

func linkAllPeers(peers []*aw.Peer) {
	for i := range peers {
		for j := range peers {
			if i != j {
				peers[i].Link(peers[j].Self)
				peers[j].Link(peers[i].Self)
			}
		}
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
