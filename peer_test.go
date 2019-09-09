package aw_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/phi/co"
	"github.com/sirupsen/logrus"
)

const CAPACITY = 1000

var _ = Describe("airwaves peer", func() {
	initSignVerifiers := func(nodeCount int) []testutil.MockSignVerifier {
		SignVerifiers := make([]testutil.MockSignVerifier, nodeCount)
		for i := range SignVerifiers {
			SignVerifiers[i] = testutil.NewMockSignVerifier()
		}
		for i := range SignVerifiers {
			for j := range SignVerifiers {
				SignVerifiers[i].Whitelist(SignVerifiers[j].ID())
			}
		}
		return SignVerifiers
	}

	startNodes := func(ctx context.Context, logger logrus.FieldLogger, signVerifiers []testutil.MockSignVerifier, nodeCount int) (PeerAddresses, error) {
		codec := testutil.NewSimpleTCPPeerAddressCodec()
		peerAddresses := make([]PeerAddress, nodeCount)
		for i := range peerAddresses {
			peerAddresses[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("bootstrap_%d", i), "127.0.0.1", fmt.Sprintf("%d", 46532+i))
		}
		co.ParForAll(peerAddresses, func(i int) {
			events := make(chan protocol.Event, 10)
			options := PeerOptions{
				Logger: logger.WithField("bootstrap", i),
				Codec:  codec,

				Me:                 peerAddresses[i],
				BootstrapAddresses: testutil.Remove(peerAddresses, i),
			}
			if signVerifiers != nil && len(signVerifiers) == len(peerAddresses) {
				options.SignVerifier = signVerifiers[i]
			}
			go NewTCPPeer(options, events, CAPACITY, 46532+i).Run(ctx)
		})
		return peerAddresses, nil
	}

	tableNodeCount := []struct {
		TotalBootstrap int
		KnownBootstrap int

		NewNodes int
	}{
		// When all the nodes are known
		{4, 4, 4},
		// {10, 10, 10},
		// {20, 20, 10},
		// {40, 40, 40},

		// When half of nodes are known
		{4, 2, 4},
		// {10, 5, 10},
		// {20, 10, 20},
		// {40, 20, 40},

		// When one node is known
		{4, 1, 4},
		// {10, 1, 10},
		// {20, 1, 20},
		// {40, 1, 40},
	}

	Context("when bootstrapping", func() {
		for _, nodeCount := range tableNodeCount {
			nodeCount := nodeCount
			It(fmt.Sprintf(
				"should connect to %d nodes when %d nodes are known, when new nodes join sequentially",
				nodeCount.TotalBootstrap+nodeCount.NewNodes-1, nodeCount.KnownBootstrap), func() {
				logger := logrus.StandardLogger()
				SignVerifiers := initSignVerifiers(nodeCount.TotalBootstrap + nodeCount.NewNodes)
				bootstrapSignVerifiers := make([]testutil.MockSignVerifier, nodeCount.TotalBootstrap)
				nodeSignVerifiers := make([]testutil.MockSignVerifier, nodeCount.NewNodes)
				copy(bootstrapSignVerifiers, SignVerifiers[:nodeCount.TotalBootstrap])
				copy(nodeSignVerifiers, SignVerifiers[nodeCount.TotalBootstrap:])

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				bootstrapAddrs, err := startNodes(ctx, logger, bootstrapSignVerifiers, nodeCount.TotalBootstrap)
				Expect(err).Should(BeNil())

				peerAddresses := make([]PeerAddress, nodeCount.NewNodes)
				for i := range peerAddresses {
					peerAddresses[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("test_node_%d", i), "127.0.0.1", fmt.Sprintf("%d", 5000+i))
				}
				codec := testutil.NewSimpleTCPPeerAddressCodec()
				peers := make([]Peer, nodeCount.NewNodes)
				for i, peerAddr := range peerAddresses {
					events := make(chan protocol.Event, 10)
					peer := NewTCPPeer(PeerOptions{
						Logger: logger.WithField("test_node", i),
						Codec:  codec,

						SignVerifier:       nodeSignVerifiers[i],
						Me:                 peerAddr,
						BootstrapAddresses: bootstrapAddrs,
					}, events, CAPACITY, 5000+i)
					go peer.Run(ctx)
					peers[i] = peer
				}

				// wait for the nodes to bootstrap
				time.Sleep(time.Minute)

				for _, peer := range peers {
					val, err := peer.NumPeers(ctx)
					Expect(err).Should(BeNil())
					Expect(val).Should(Equal(nodeCount.TotalBootstrap + nodeCount.NewNodes - 1))
				}
			})
		}
	})

	Context("when updating peer address", func() {
		FIt("should be able to send messages to the new address", func() {
			logger := logrus.StandardLogger()

			peer1Events := make(chan protocol.Event, 65535)
			peer2Events := make(chan protocol.Event, 65535)
			updatedPeer2Events := make(chan protocol.Event, 65535)

			codec := testutil.NewSimpleTCPPeerAddressCodec()
			peer1Address := testutil.NewSimpleTCPPeerAddress("peer_1", "127.0.0.1", fmt.Sprintf("%d", 8080))
			peer2Address := testutil.NewSimpleTCPPeerAddress("peer_2", "127.0.0.1", fmt.Sprintf("%d", 8081))
			updatedPeer2Address := testutil.NewSimpleTCPPeerAddress("peer_2", "127.0.0.1", fmt.Sprintf("%d", 8082))
			updatedPeer2Address.Nonce = 1

			peer1 := NewTCPPeer(PeerOptions{
				Logger:             logger,
				Me:                 peer1Address,
				BootstrapAddresses: PeerAddresses{peer2Address},
				Codec:              codec,

				BootstrapDuration: 3 * time.Second,
			}, peer1Events, 65535, 8080)

			peer2 := NewTCPPeer(PeerOptions{
				Logger:             logger,
				Me:                 peer2Address,
				BootstrapAddresses: PeerAddresses{peer1Address},
				Codec:              codec,

				BootstrapDuration: 3 * time.Second,
			}, peer2Events, 65535, 8081)

			updatedPeer2 := NewTCPPeer(PeerOptions{
				Logger:             logger,
				Me:                 updatedPeer2Address,
				BootstrapAddresses: PeerAddresses{peer1Address},
				Codec:              codec,

				BootstrapDuration: 3 * time.Second,
			}, updatedPeer2Events, 65535, 8082)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			go func() {
				<-ctx.Done()
				cancel()
			}()

			co.ParBegin(
				func() {
					peer1.Run(context.Background())
				},
				func() {
					peer2.Run(ctx)
					fmt.Println("peer 2 restarted")
					updatedPeer2.Run(context.Background())
				},
				func() {
					<-ctx.Done()
					ctx2, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					go func() {
						<-ctx2.Done()
						cancel()
					}()
					if err := peer1.Cast(ctx2, testutil.SimplePeerID("peer_2"), []byte("hello")); err != nil {
						panic(err)
					}
				},
				func() {
					for event := range peer1Events {
						fmt.Println(event)
					}
				},
				func() {
					for event := range updatedPeer2Events {
						fmt.Println(event)
					}
				},
			)
		})
	})
})
