package peer_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/phi"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Peer", func() {

	castTest := func(ctx context.Context, peers []peer.Peer, events []chan protocol.Event) {
		castCtx, castCancel := context.WithTimeout(ctx, 10*time.Second)
		defer castCancel()

		sender, receiver := RandomSenderAndReceiver(len(peers))
		messageBody := RandomMessageBody()
		Expect(peers[sender].Cast(castCtx, peers[receiver].Me().PeerID(), messageBody)).NotTo(HaveOccurred())

		message, ok := ReadChannel(castCtx, events[receiver])
		Expect(ok).Should(BeTrue())
		Expect(message.From.Equal(peers[sender].Me().PeerID())).Should(BeTrue())
		Expect(bytes.Equal(message.Message, messageBody)).Should(BeTrue())
	}

	multicastTest := func(ctx context.Context, peers []peer.Peer, events []chan protocol.Event) {
		multicastCtx, multicastCancel := context.WithTimeout(ctx, 10*time.Second)
		defer multicastCancel()

		sender := rand.Intn(len(peers))
		messageBody := RandomMessageBody()
		Expect(peers[sender].Multicast(multicastCtx, protocol.NilPeerGroupID, messageBody)).NotTo(HaveOccurred())

		for i, event := range events {
			if i == sender {
				continue
			}
			message, ok := ReadChannel(multicastCtx, event)
			Expect(ok).Should(BeTrue())
			Expect(message.From.Equal(peers[sender].Me().PeerID())).Should(BeTrue())
			Expect(bytes.Equal(message.Message, messageBody)).Should(BeTrue())
		}
	}

	broadcastTest := func(ctx context.Context, peers []peer.Peer, events []chan protocol.Event, messageBody []byte) bool {
		broadcastCtx, broadcastCancel := context.WithTimeout(ctx, 10*time.Second)
		defer broadcastCancel()

		sender := rand.Intn(len(peers))
		Expect(peers[sender].Broadcast(broadcastCtx, protocol.NilPeerGroupID, messageBody)).NotTo(HaveOccurred())

		for i, event := range events {
			if i == sender {
				continue
			}
			message, ok := ReadChannel(broadcastCtx, event)
			Expect(ok).Should(BeTrue())
			Expect(bytes.Equal(message.Message, messageBody)).Should(BeTrue())
		}
		return true
	}

	Context("single group network", func() {
		networkOption := []struct {
			B int // num of bootstrap nodes
			N int // total num of nodes(including bootstrap nodes)
		}{
			{6, 24},
		}

		for _, option := range networkOption {
			b, n := option.B, option.N

			Context(fmt.Sprintf("when network have %v peers and %v of them are bootstrap nodes", n, b), func() {
				It("should connect to all other nodes in the network", func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer func() {
						cancel()
						time.Sleep(2 * time.Second)
					}()

					// Start all bootstrap nodes and all other nodes 3 seconds later.
					peers, events := NewFullyConnectedPeers(b, n)
					go phi.ParForAll(peers, func(i int) {
						if i >= b {
							time.Sleep(3 * time.Second)
						}
						peers[i].Run(ctx)
					})

					// Expect they all connect to each other
					Eventually(func() bool {
						for _, peer := range peers {
							numPeers, err := peer.NumPeers()
							if err != nil {
								return false
							}
							if numPeers < n-1 {
								return false
							}
						}
						return true
					}, 2*time.Minute, 3*time.Second).Should(BeTrue())
					logrus.Print("All nodes are connected to each other.")

					// Expect cast is working as expected.
					logrus.Print("Testing cast messages...")
					cast := func() bool {
						castTest(ctx, peers, events)
						return true
					}
					Expect(quick.Check(cast, nil)).NotTo(HaveOccurred())

					// Expect multicast is working as expected
					logrus.Print("Testing multicast messages...")
					multicast := func() bool {
						multicastTest(ctx, peers, events)
						return true
					}
					Expect(quick.Check(multicast, nil)).NotTo(HaveOccurred())

					// Expect multicast is working as expected
					logrus.Print("Testing broadcast messages...")
					messages := map[string]struct{}{}
					broadcast := func() bool {
						message := RandomMessageBody()
						for {
							if _, ok := messages[string(message)]; !ok {
								break
							}
							message = RandomMessageBody()
						}
						messages[string(message)] = struct{}{}
						return broadcastTest(ctx, peers, events, message)
					}
					Expect(quick.Check(broadcast, nil)).NotTo(HaveOccurred())
				})
			})
		}
	})
})
