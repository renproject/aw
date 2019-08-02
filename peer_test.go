package aw_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/phi/co"
	"github.com/sirupsen/logrus"
)

var _ = Describe("airwaves peer", func() {
	initServer := func(ctx context.Context, bind string, sender protocol.MessageSender, sv protocol.SignVerifier) {
		err := tcp.NewServer(tcp.ServerOptions{
			Logger:  logrus.StandardLogger(),
			Timeout: time.Minute,
		}, sender, handshake.New(sv)).Listen(ctx, bind)
		if err != nil {
			panic(err)
		}
	}

	initClient := func(ctx context.Context, receiver protocol.MessageReceiver, sv protocol.SignVerifier) {
		tcp.NewClient(
			tcp.NewClientConns(tcp.ClientOptions{
				Logger:         logrus.StandardLogger(),
				Timeout:        time.Minute,
				MaxConnections: 10,
			}, handshake.New(sv)),
			receiver,
		).Run(ctx)
	}

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

	startNodes := func(ctx context.Context, SignVerifiers []testutil.MockSignVerifier, nodeCount int) (PeerAddresses, error) {
		codec := testutil.NewSimpleTCPPeerAddressCodec()
		peerAddresses := make([]PeerAddress, nodeCount)

		for i := range peerAddresses {
			peerAddresses[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("bootstrap_%d", i), "127.0.0.1", fmt.Sprintf("%d", 46532+i))
		}

		co.ParForAll(peerAddresses, func(i int) {
			serverMessages := make(chan protocol.MessageOnTheWire, 10)
			clientMessages := make(chan protocol.MessageOnTheWire, 10)
			events := make(chan protocol.Event, 10)
			bootstrapAddrs := testutil.Remove(peerAddresses, i)
			go initServer(ctx, peerAddresses[i].NetworkAddress().String(), serverMessages, SignVerifiers[i])
			go initClient(ctx, clientMessages, SignVerifiers[i])

			peer := Default(PeerOptions{
				Me:                 peerAddresses[i],
				BootstrapAddresses: bootstrapAddrs,
				Codec:              codec,

				Logger: logrus.StandardLogger(),
			}, serverMessages, clientMessages, events)

			go co.ParBegin(
				func() {
					for {
						select {
						case <-ctx.Done():
							return
						case <-events:
						}
					}
				},
				func() {
					peer.Run(ctx)
				},
			)
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

		// // When half of nodes are known
		{4, 2, 4},
		// {10, 5, 10},
		// {20, 10, 20},
		// {40, 20, 40},

		// // When one node is known
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
				nodeCount.TotalBootstrap+nodeCount.NewNodes, nodeCount.KnownBootstrap), func() {

				SignVerifiers := initSignVerifiers(nodeCount.TotalBootstrap + nodeCount.NewNodes)
				bootstrapSignVerifiers := make([]testutil.MockSignVerifier, nodeCount.TotalBootstrap)
				nodeSignVerifiers := make([]testutil.MockSignVerifier, nodeCount.NewNodes)
				copy(bootstrapSignVerifiers, SignVerifiers[:nodeCount.TotalBootstrap])
				copy(nodeSignVerifiers, SignVerifiers[nodeCount.TotalBootstrap:])

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				bootstrapAddrs, err := startNodes(ctx, bootstrapSignVerifiers, nodeCount.TotalBootstrap)
				Expect(err).Should(BeNil())

				peerAddresses := make([]PeerAddress, nodeCount.NewNodes)
				for i := range peerAddresses {
					peerAddresses[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("test_node_%d", i), "127.0.0.1", fmt.Sprintf("%d", 5000+i))
				}
				codec := testutil.NewSimpleTCPPeerAddressCodec()

				peers := make([]Peer, nodeCount.NewNodes)
				for i, peerAddr := range peerAddresses {
					serverMessages := make(chan protocol.MessageOnTheWire, 10)
					clientMessages := make(chan protocol.MessageOnTheWire, 10)
					events := make(chan protocol.Event, 10)

					me := peerAddr

					go initServer(ctx, me.NetworkAddress().String(), serverMessages, nodeSignVerifiers[i])
					go initClient(ctx, clientMessages, nodeSignVerifiers[i])

					peer := Default(PeerOptions{
						Me:                 me,
						BootstrapAddresses: bootstrapAddrs[:nodeCount.KnownBootstrap],
						Codec:              codec,

						Logger: logrus.StandardLogger(),
					}, serverMessages, clientMessages, events)

					go peer.Run(ctx)
					go func() {
						for {
							select {
							case <-ctx.Done():
								return
							case <-events:
							}
						}
					}()
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

	Context("when casting", func() {

	})
})
