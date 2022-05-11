package aw_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/renproject/aw"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
	"go.uber.org/zap"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*
 * TODO(ross):
 *
 * Cases missing in coverage report:
 *     - Multiple syncs outstanding for the same content
 *     - Failed calls to `handleSendMessage`
 *     - Dropped reader for ephemeral connections
 *     - Dropped writer
 *     - Upgrading an ephemeral connection to a linked peer
 *     - Tear down connection when in the process of sending a message
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

			err = peer1.GossipNonBlocking(contentID, nil)
			if err != nil {
				panic(err)
			}

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

			err = peer1.GossipNonBlocking(contentID, nil)
			if err != nil {
				panic(err)
			}

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
			err = peers[0].GossipNonBlocking(contentID, nil)
			if err != nil {
				panic(err)
			}

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
			peer1.PeerTable.AddPeer(peer2.ID(), address2)

			_, err = peer1.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}
			go peer1.Run(ctx)

			err = peer1.GossipNonBlocking(contentID, nil)
			if err != nil {
				panic(err)
			}

			time.Sleep(50 * time.Millisecond)

			_, err = peer2.Listen(ctx, fmt.Sprintf("localhost:%v", peer2Port))
			if err != nil {
				panic(err)
			}
			go peer2.Run(ctx)

			Eventually(func() []byte { msg, _ := peer2.ContentResolver.QueryContent(contentID); return msg }, 1000).Should(Equal(content))
		})

		Specify("only peers in a given subnet should receive content when gossiping to that subnet", func() {
			n := 10

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)

			peers := make([]*aw.Peer, n)
			for i := range peers {
				peers[i] = newPeerAndListen(ctx, opts)
			}

			connectAllPeers(peers)

			for _, peer := range peers {
				go peer.Run(ctx)
			}

			linkAllPeers(peers)

			// Subnet that consists of the first half of the peers.
			subnetPeers := make([]id.Signatory, n/2)
			for i := range subnetPeers {
				subnetPeers[i] = peers[i].ID()
			}

			var subnet id.Hash
			for i := range subnetPeers {
				subnet = peers[i].PeerTable.AddSubnet(subnetPeers)
			}

			contentID := []byte("id")
			data := []byte("hello")
			peers[0].ContentResolver.InsertContent(contentID, data)
			err = peers[0].GossipNonBlocking(contentID, &subnet)
			if err != nil {
				panic(err)
			}

			// Give more of an opportunity for the gossip to reach everyone.
			time.Sleep(50 * time.Millisecond)

			for i, peer := range peers {
				if i < len(subnetPeers) {
					Eventually(func() []byte { msg, _ := peer.ContentResolver.QueryContent(contentID); return msg }).Should(Equal(data))
				} else {
					_, exists := peer.ContentResolver.QueryContent(contentID)
					Expect(exists).To(BeFalse())
				}
			}
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
				peerIDs[i] = peers[i].ID()
			}

			hasDiscoveredAllOthers := func(peer *aw.Peer, ids []id.Signatory) bool {
				id := peer.ID()
				for _, other := range ids {
					if !other.Equal(&id) {
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

		It("should return the same content if it is already present", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)

			peer1 := newPeerAndListen(ctx, opts)
			peer2 := newPeerAndListen(ctx, opts)
			peers := []*aw.Peer{peer1, peer2}

			connectAllPeers(peers)

			go peer1.Run(ctx)
			go peer2.Run(ctx)

			linkAllPeers(peers)

			contentID := []byte("id")
			rightContent := []byte("right")
			wrongContent := []byte("wrong")

			// NOTE: In practice this could occur if a malicious node sent one
			// content for the first sync and different content for the second
			// sync (but with the same content ID).
			peer1.ContentResolver.InsertContent(contentID, rightContent)
			peer2.ContentResolver.InsertContent(contentID, wrongContent)

			syncCtx, syncCancel := context.WithTimeout(ctx, time.Second)
			receivedData, err := peer1.Sync(syncCtx, contentID, nil)
			syncCancel()

			Expect(err).To(BeNil())
			Expect(receivedData).To(Equal(rightContent))
		})

		It("should expire old pending syncs", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opts := defaultOptions(logger)
			opts.MaxPendingSyncs = 10

			peer1 := newPeerAndListen(ctx, opts)
			peer2 := newPeerAndListen(ctx, opts)
			peers := []*aw.Peer{peer1, peer2}

			connectAllPeers(peers)

			go peer1.Run(ctx)
			go peer2.Run(ctx)

			linkAllPeers(peers)

			successID := []byte("success")
			peer2.ContentResolver.InsertContent(successID, []byte("content"))

			for i := 0; i < int(opts.MaxPendingSyncs); i++ {
				i := i
				go func() {
					contentID := []byte(fmt.Sprintf("%v", i))
					syncCtx, syncCancel := context.WithTimeout(ctx, 100*time.Millisecond)
					peer1.Sync(syncCtx, contentID, nil)
					syncCancel()
				}()
			}

			time.Sleep(100 * time.Millisecond)

			syncCtx, syncCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			_, err := peer1.Sync(syncCtx, successID, nil)
			syncCancel()

			Expect(err).ToNot(HaveOccurred())
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

				hint := peers[0].ID()

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

			peer1 := newPeerWithReceiver(ctx, opts, nil)
			peer2 := newPeerWithReceiver(ctx, opts, receive)

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
			peer1.PeerTable.AddPeer(peer2.ID(), addr2)
			peer2.PeerTable.AddPeer(peer1.ID(), addr1)

			go peer1.Run(ctx)
			go peer2.Run(ctx)

			err = peer1.Link(peer2.ID())
			if err != nil {
				panic(err)
			}
			err = peer2.Link(peer1.ID())
			if err != nil {
				panic(err)
			}

			msg := []byte("hello")
			sendCtx, sendCancel := context.WithTimeout(ctx, time.Second)
			err = peer1.Send(sendCtx, msg, peer2.ID())
			sendCancel()

			Expect(err).To(BeNil())
			Eventually(resultCh).Should(Receive(Equal(result{peer1.ID(), msg})))
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stablePeer := newPeerAndListen(ctx, opts)
		crashPeer := newPeerAndListen(ctx, opts)

		stableAddress := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", stablePeer.Port), uint64(time.Now().UnixNano()))
		crashAddress := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", crashPeer.Port), uint64(time.Now().UnixNano()))
		stablePeer.PeerTable.AddPeer(crashPeer.ID(), crashAddress)
		crashPeer.PeerTable.AddPeer(stablePeer.ID(), stableAddress)

		go stablePeer.Run(ctx)
		go crashPeer.Run(ctx)

		err = stablePeer.Link(crashPeer.ID())
		if err != nil {
			panic(err)
		}
		err = crashPeer.Link(stablePeer.ID())
		if err != nil {
			panic(err)
		}

		numMessages := 20
		for i := 0; i < numMessages; i++ {
			contentID := []byte(fmt.Sprintf("%v", i))
			content := []byte(fmt.Sprintf("message %v", i))
			stablePeer.ContentResolver.InsertContent(contentID, content)
			err = stablePeer.Gossip(ctx, contentID, nil)
			if err != nil {
				panic(err)
			}

			if i == numMessages/2 {
				// NOTE(ross): We don't check that the content was received
				// because there is always a chance that it won't be; this can
				// happen if the connection is killed at any point after the
				// stable peer sends the gossip and before the sync is handled
				// by the crash peer.
				err = crashPeer.Unlink(stablePeer.ID())
				if err != nil {
					panic(err)
				}
				err = crashPeer.Link(stablePeer.ID())
				if err != nil {
					panic(err)
				}

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

	Context("rate limiting", func() {
		It("should rate limit incoming connections", func() {
			opts := defaultOptions(logger)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			peer := newPeerAndListen(ctx, opts)
			go peer.Run(ctx)

			buf := [1]byte{}
			addr := fmt.Sprintf("%v:%v", "localhost", peer.Port)
			deadline := time.Now().Add(time.Second)
			for {
				if time.Now().After(deadline) {
					Fail("connections were not rate limited")
				} else {
					dialer := new(net.Dialer)

					conn, err := dialer.DialContext(ctx, "tcp", addr)
					if err != nil {
						// NOTE(ross): A smarter implementation might actually
						// cause a dial error when the address is rate limited, so
						// this might need to change in the future.
						panic(err)
					}

					// Once a new connection has been accepted, the handshake
					// should start. There should therefore be some data sent if
					// the connection attempt was not rate limited.
					_, err = io.ReadFull(conn, buf[:])
					if err != nil {
						break
					}
				}
			}
		})

		It("should rate limit incoming data for a given connection", func() {
			opts := defaultOptions(logger)
			opts.MaxMessageSize = 2 * 1024
			opts.ConnectionRateLimiterOptions.Rate = 1024
			opts.ConnectionRateLimiterOptions.Burst = 2 * 1024

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			writePeer := newPeerAndListen(ctx, opts)

			received := make(chan struct{}, 1)
			readPeer := newPeerWithReceiver(ctx, opts, func(_ id.Signatory, _ []byte) {
				received <- struct{}{}
			})
			_, err := readPeer.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}

			writeAddress := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", writePeer.Port), uint64(time.Now().UnixNano()))
			readAddress := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", readPeer.Port), uint64(time.Now().UnixNano()))
			writePeer.PeerTable.AddPeer(readPeer.ID(), readAddress)
			readPeer.PeerTable.AddPeer(writePeer.ID(), writeAddress)

			go writePeer.Run(ctx)
			go readPeer.Run(ctx)

			err = writePeer.Link(readPeer.ID())
			if err != nil {
				panic(err)
			}
			err = readPeer.Link(writePeer.ID())
			if err != nil {
				panic(err)
			}

			data := make([]byte, int(opts.ConnectionRateLimiterOptions.Rate))

			err = writePeer.Send(ctx, data, readPeer.ID())
			if err != nil {
				panic(err)
			}
			Eventually(received).Should(Receive())
			err = writePeer.Send(ctx, data, readPeer.ID())
			if err != nil {
				panic(err)
			}
			Consistently(received).ShouldNot(Receive())
		})
	})

	Context("peer management", func() {
		It("should limit the number of concurrent linked peers", func() {
			opts := defaultOptions(logger)
			opts.MaxLinkedPeers = 10

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			peer := newPeer(opts)
			go peer.Run(ctx)

			for i := uint(0); i < opts.MaxLinkedPeers; i++ {
				privKey := id.NewPrivKey()
				Expect(peer.Link(privKey.Signatory())).To(Succeed())
			}

			privKey := id.NewPrivKey()
			Expect(peer.Link(privKey.Signatory())).ToNot(Succeed())
		})

		It("should limit the number of ephemeral connections", func() {
			opts := defaultOptions(logger)
			opts.MaxEphemeralConnections = 10

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			received := make(chan struct{}, 1)
			peer := newPeerWithReceiver(ctx, opts, func(_ id.Signatory, _ []byte) {
				received <- struct{}{}
			})
			_, err := peer.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}
			go peer.Run(ctx)

			addr := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", peer.Port), uint64(time.Now().UnixNano()))

			otherPeers := make([]*aw.Peer, opts.MaxEphemeralConnections)
			for i := range otherPeers {
				otherPeers[i] = newPeerAndListen(ctx, opts)
				otherPeers[i].PeerTable.AddPeer(peer.ID(), addr)
				go otherPeers[i].Run(ctx)
			}

			lastPeer := newPeerAndListen(ctx, opts)
			lastPeer.PeerTable.AddPeer(peer.ID(), addr)
			go lastPeer.Run(ctx)

			for _, otherPeer := range otherPeers {
				err = otherPeer.Link(peer.ID())
				if err != nil {
					panic(err)
				}
			}

			// Give some time for the other peers to finish linking.
			time.Sleep(50 * time.Millisecond)

			err = lastPeer.Send(ctx, []byte{0}, peer.ID())
			if err != nil {
				panic(err)
			}

			Consistently(received).ShouldNot(Receive())
		})
	})

	Context("invalid message handling", func() {
		It("should not receive invalid messages", func() {
			// NOTE(ross): There is no good way of testing if the peer is
			// correctly discarding the bad messages, so the following mostly
			// serves to get some coverage of these branches that will
			// hopefully catch any particularly egregious problems.

			opts := defaultOptions(logger)
			opts.MaxEphemeralConnections = 10

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			received := make(chan struct{}, 1)
			peer := newPeerWithReceiver(ctx, opts, func(_ id.Signatory, _ []byte) {
				received <- struct{}{}
			})
			_, err := peer.Listen(ctx, "localhost:0")
			if err != nil {
				panic(err)
			}
			go peer.Run(ctx)

			peerSignatory := peer.ID()

			// Make sure we are not the keep alive decider.
			var dialerPrivKey *id.PrivKey
			for {
				dialerPrivKey = id.NewPrivKey()
				dialerSignatory := dialerPrivKey.Signatory()
				if bytes.Compare(dialerSignatory[:], peerSignatory[:]) < 0 {
					break
				}
			}
			dialer := new(net.Dialer)
			conn, err := dialer.Dial("tcp", fmt.Sprintf("localhost:%v", peer.Port))
			if err != nil {
				panic(err)
			}

			_, _, err = handshake.Handshake(dialerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())
			buf := make([]byte, 1024)

			// Completely random message.
			_, err = rand.Read(buf)
			if err != nil {
				panic(err)
			}
			_, err = conn.Write(buf)
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(10 * time.Millisecond)

			// Valid length prefix.
			conn, err = dialer.Dial("tcp", fmt.Sprintf("localhost:%v", peer.Port))
			if err != nil {
				panic(err)
			}
			_, _, err = handshake.Handshake(dialerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())

			binary.BigEndian.PutUint32(buf, 100)
			_, err = conn.Write(buf)
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(10 * time.Millisecond)

			// Valid encoding of random data.
			conn, err = dialer.Dial("tcp", fmt.Sprintf("localhost:%v", peer.Port))
			if err != nil {
				panic(err)
			}
			session, _, err := handshake.Handshake(dialerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())

			_, err = rand.Read(buf)
			if err != nil {
				panic(err)
			}
			nonceBuf := session.GetWriteNonceAndIncrement()
			sealed := session.GCM.Seal(buf[4:4], nonceBuf[:], buf[4:10], nil)
			binary.BigEndian.PutUint32(buf, uint32(len(sealed)))
			_, err = conn.Write(buf[:4+len(sealed)])
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(10 * time.Millisecond)

			// Valid encoding of unexpected sync.
			conn, err = dialer.Dial("tcp", fmt.Sprintf("localhost:%v", peer.Port))
			if err != nil {
				panic(err)
			}
			session, _, err = handshake.Handshake(dialerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())

			_, err = rand.Read(buf)
			if err != nil {
				panic(err)
			}
			msg := wire.Msg{
				Version: wire.MsgVersion1,
				Type:    wire.MsgTypeSync,
				Data:    buf[:32],
			}
			bs, err := surge.ToBinary(msg)
			Expect(err).ToNot(HaveOccurred())
			nonceBuf = session.GetWriteNonceAndIncrement()
			sealed = session.GCM.Seal(buf[4:4], nonceBuf[:], bs, nil)
			binary.BigEndian.PutUint32(buf, uint32(len(sealed)))
			_, err = conn.Write(buf[:4+len(sealed)])
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(10 * time.Millisecond)

			// Valid encoding of expected sync with invalid data.
			conn, err = dialer.Dial("tcp", fmt.Sprintf("localhost:%v", peer.Port))
			if err != nil {
				panic(err)
			}
			session, _, err = handshake.Handshake(dialerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())

			id := [32]byte{}
			_, err = rand.Read(id[:])
			if err != nil {
				panic(err)
			}
			msg = wire.Msg{
				Version: wire.MsgVersion1,
				Type:    wire.MsgTypePush,
				To:      aw.DefaultSubnet,
				Data:    id[:],
			}
			bs, err = surge.ToBinary(msg)
			Expect(err).ToNot(HaveOccurred())
			nonceBuf = session.GetWriteNonceAndIncrement()
			sealed = session.GCM.Seal(buf[4:4], nonceBuf[:], bs, nil)
			binary.BigEndian.PutUint32(buf, uint32(len(sealed)))
			_, err = conn.Write(buf[:4+len(sealed)])
			Expect(err).ToNot(HaveOccurred())

			// Wait for the pull but we don't care about reading it fully.
			_, err = conn.Read(buf[:1])
			Expect(err).ToNot(HaveOccurred())

			_, err = rand.Read(buf)
			if err != nil {
				panic(err)
			}
			msg = wire.Msg{
				Version:  wire.MsgVersion1,
				Type:     wire.MsgTypeSync,
				To:       aw.DefaultSubnet,
				Data:     id[:],
				SyncData: buf[:100],
			}
			bs, err = surge.ToBinary(msg)
			Expect(err).ToNot(HaveOccurred())
			nonceBuf = session.GetWriteNonceAndIncrement()
			sealed = session.GCM.Seal(buf[4:4], nonceBuf[:], bs, nil)
			binary.BigEndian.PutUint32(buf, uint32(len(sealed)))
			_, err = conn.Write(buf[:4+len(sealed)])
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(10 * time.Millisecond)
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

func newPeerWithReceiver(ctx context.Context, opts aw.Options, receiver func(id.Signatory, []byte)) *aw.Peer {
	privKey := id.NewPrivKey()
	peerTable := dht.NewInMemTable(privKey.Signatory())
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	peer := aw.New(opts, privKey, peerTable, contentResolver)
	go receive(ctx, peer, receiver)

	return peer
}

func receive(ctx context.Context, peer *aw.Peer, receiver func(id.Signatory, []byte)) {
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP

		case msg := <-peer.IncomingMessages:
			receiver(msg.From, msg.Data)
		}
	}
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
				peers[i].PeerTable.AddPeer(peers[j].ID(), addresses[j])
				peers[j].PeerTable.AddPeer(peers[i].ID(), addresses[i])
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
		peers[i].PeerTable.AddPeer(peers[next].ID(), addresses[next])
		peers[next].PeerTable.AddPeer(peers[i].ID(), addresses[i])
	}
}

func linkAllPeers(peers []*aw.Peer) {
	for i := range peers {
		for j := range peers {
			if i != j {
				err := peers[i].Link(peers[j].ID())
				if err != nil {
					panic(err)
				}
				err = peers[j].Link(peers[i].ID())
				if err != nil {
					panic(err)
				}
			}
		}
	}
}

func linkPeersRing(peers []*aw.Peer) {
	n := len(peers)

	for i := range peers {
		next := (i + 1) % n
		err := peers[i].Link(peers[next].ID())
		if err != nil {
			panic(err)
		}
		err = peers[next].Link(peers[i].ID())
		if err != nil {
			panic(err)
		}
	}
}

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
