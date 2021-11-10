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
 *
 * Cases missing in coverage report:
 *     - Bad message data
 *     - Multiple syncs outstanding for the same content
 *     - Failed calls to `handleSendMessage`
 *     - Dropped reader for ephemeral connections
 *     - Dropped writer
 *     - Too many linked peers/ephemeral connections
 *     - Upgrading an ephemeral connection to a linked peer
 *     - Gossiping to a particular subnet
 *     - Tear down connection when in the process of sending a message
 *     - Rate limiting/filtering
 *
 * Replace these exmaple programs?
 *     - Peers in a ring topology, all gossip a unique message over many
 *       rounds; track throughput and make sure all messages are received
 *     - Many rounds of syncing a message from a peer (hint is given) and
 *       success and throughput is checked
 */

func defaultOptions(logger *zap.Logger) aw.Options {
	options := aw.DefaultOptions()
	options.Logger = logger

	return options
}

var _ = Describe("Peer", func() {
	loggerConfig := zap.NewDevelopmentConfig()
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

			peer1.GossipNonBlocking(contentID, nil)

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

			peer1.GossipNonBlocking(contentID, nil)

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

			peers[0].GossipNonBlocking(contentID, nil)

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
			peer1.PeerTable.AddPeer(peer2.ID, address2)

			_, err = peer1.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}
			go peer1.Run(ctx)

			peer1.GossipNonBlocking(contentID, nil)

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
				peerIDs[i] = peers[i].ID
			}

			hasDiscoveredAllOthers := func(peer *aw.Peer, ids []id.Signatory) bool {
				for _, other := range ids {
					if !other.Equal(&peer.ID) {
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

			syncCtx, syncCancel := context.WithTimeout(ctx, time.Second)
			receivedData, err := peers[1].Sync(syncCtx, contentID, nil)
			syncCancel()

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

				hint := peers[0].ID

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

	Context("direct messaging", func() {
		Specify("peers should call the user defined receiver function for direct messages", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)

			type result struct {
				remote id.Signatory
				msg    []byte
			}

			resultCh := make(chan result, 1)
			receive := func(remote id.Signatory, msg []byte) {
				resultCh <- result{remote, msg}
			}

			peer1 := newPeerWithReceiver(opts, nil)
			peer2 := newPeerWithReceiver(opts, receive)

			_, err := peer1.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}
			_, err = peer2.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}

			addr1 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", peer1.Port), uint64(time.Now().UnixNano()))
			addr2 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", peer2.Port), uint64(time.Now().UnixNano()))
			peer1.PeerTable.AddPeer(peer2.ID, addr2)
			peer2.PeerTable.AddPeer(peer1.ID, addr1)

			go peer1.Run(ctx)
			go peer2.Run(ctx)

			peer1.Link(peer2.ID)
			peer2.Link(peer1.ID)

			msg := []byte("hello")
			sendCtx, sendCancel := context.WithTimeout(ctx, time.Second)
			err = peer1.Send(sendCtx, msg, peer2.ID)
			sendCancel()

			Expect(err).To(BeNil())
			Eventually(resultCh).Should(Receive(Equal(result{peer1.ID, msg})))
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

	It("should not drop messages when the connection changes", func() {
		opts := defaultOptions(logger)
		opts.OutgoingBufferSize = 100

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stablePeer := newPeerAndListen(ctx, opts)
		crashPeer := newPeerAndListen(ctx, opts)

		stableAddress := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", stablePeer.Port), uint64(time.Now().UnixNano()))
		crashAddress := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", crashPeer.Port), uint64(time.Now().UnixNano()))
		stablePeer.PeerTable.AddPeer(crashPeer.ID, crashAddress)
		crashPeer.PeerTable.AddPeer(stablePeer.ID, stableAddress)

		go stablePeer.Run(ctx)
		go crashPeer.Run(ctx)

		stablePeer.Link(crashPeer.ID)
		crashPeer.Link(stablePeer.ID)

		numMessages := 20
		for i := 0; i < numMessages; i++ {
			contentID := []byte(fmt.Sprintf("%v", i))
			content := []byte(fmt.Sprintf("message %v", i))
			stablePeer.ContentResolver.InsertContent(contentID, content)
			stablePeer.Gossip(ctx, contentID, nil)

			if i == numMessages/2 {
				// NOTE(ross): We don't check that the content was received
				// because there is always a chance that it won't be; this can
				// happen if the connection is killed at any point after the
				// stable peer sends the gossip and before the sync is handled
				// by the crash peer.
				crashPeer.Unlink(stablePeer.ID)
				crashPeer.Link(stablePeer.ID)

				// NOTE(ross): Due to scheduling, it is possible that the next
				// loop can begin and the stable peer can have started
				// gossiping before it sees the connection as closed. This
				// would cause the test to fail. We sleep briefly here to make
				// this much less likely.
				time.Sleep(10 * time.Millisecond)
			} else {
				Eventually(func() bool {
					_, ok := crashPeer.ContentResolver.QueryContent(contentID)
					return ok
				}).Should(BeTrue())
			}
		}
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
	peer := aw.New(opts, privKey, peerTable, contentResolver, nil)

	return peer
}

func newPeerWithReceiver(opts aw.Options, receiver func(id.Signatory, []byte)) *aw.Peer {
	privKey := id.NewPrivKey()
	peerTable := dht.NewInMemTable(privKey.Signatory())
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	peer := aw.New(opts, privKey, peerTable, contentResolver, receiver)

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
				peers[i].PeerTable.AddPeer(peers[j].ID, addresses[j])
				peers[j].PeerTable.AddPeer(peers[i].ID, addresses[i])
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
		peers[i].PeerTable.AddPeer(peers[next].ID, addresses[next])
		peers[next].PeerTable.AddPeer(peers[i].ID, addresses[i])
	}
}

func linkAllPeers(peers []*aw.Peer) {
	for i := range peers {
		for j := range peers {
			if i != j {
				peers[i].Link(peers[j].ID)
				peers[j].Link(peers[i].ID)
			}
		}
	}
}

func linkPeersRing(peers []*aw.Peer) {
	n := len(peers)

	for i := range peers {
		next := (i + 1) % n
		peers[i].Link(peers[next].ID)
		peers[next].Link(peers[i].ID)
	}
}

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
