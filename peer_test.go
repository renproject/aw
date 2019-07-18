package aw_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/utils"
	"github.com/renproject/phi/co"
	"github.com/sirupsen/logrus"
)

var _ = Describe("airwaves peer", func() {
	startServer := func(ctx context.Context, bind string, sender protocol.MessageSender) {
		logger := logrus.StandardLogger()
		err := tcp.NewServer(tcp.ServerOptions{
			Logger:  logger,
			Timeout: time.Minute,
		}, sender).Listen(ctx, bind)
		if err != nil {
			panic(err)
		}
	}

	startClient := func(ctx context.Context, receiver protocol.MessageReceiver) {
		logger := logrus.StandardLogger()
		tcp.NewClient(
			tcp.NewClientConns(tcp.ClientOptions{
				Logger:         logger,
				Timeout:        time.Minute,
				MaxConnections: 10,
			}),
			receiver,
		).Run(ctx)
	}

	startNodes := func(ctx context.Context, nodeCount int) (PeerAddresses, error) {
		codec := utils.NewSimpleTCPPeerAddressCodec()
		peerAddresses := make([]PeerAddress, nodeCount)

		for i := range peerAddresses {
			peerAddresses[i] = utils.NewSimpleTCPPeerAddress(fmt.Sprintf("bootstrap_%d", i), "127.0.0.1", fmt.Sprintf("%d", 46532+i))
		}

		go co.ParForAll(peerAddresses, func(i int) {
			serverMessages := make(chan protocol.MessageOnTheWire, 10)
			clientMessages := make(chan protocol.MessageOnTheWire, 10)
			events := make(chan protocol.Event, 10)
			bootstrapAddrs := utils.Remove(peerAddresses, i)

			go startServer(ctx, peerAddresses[i].NetworkAddress().String(), serverMessages)
			go startClient(ctx, clientMessages)

			logger := logrus.StandardLogger()
			if i != 0 {
				logger.SetOutput(ioutil.Discard)
			}
			peer := Default(PeerOptions{
				Me:                 peerAddresses[i],
				BootstrapAddresses: bootstrapAddrs,
				Codec:              codec,

				Logger: logger,
			}, serverMessages, clientMessages, events)

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-events:
					}
				}
			}()

			peer.Run(ctx)
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
		// {4, 2, 4},
		// {10, 5, 10},
		// {20, 10, 20},
		// {40, 20, 40},

		// // When one node is known
		// {4, 1, 4},
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

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				bootstrapAddrs, err := startNodes(ctx, nodeCount.TotalBootstrap)
				Expect(err).Should(BeNil())

				peerAddresses := make([]PeerAddress, nodeCount.NewNodes)
				for i := range peerAddresses {
					peerAddresses[i] = utils.NewSimpleTCPPeerAddress(fmt.Sprintf("test_node_%d", i), "127.0.0.1", fmt.Sprintf("%d", 5000+i))
				}
				codec := utils.NewSimpleTCPPeerAddressCodec()

				peers := make([]Peer, nodeCount.NewNodes)
				for i, peerAddr := range peerAddresses {
					serverMessages := make(chan protocol.MessageOnTheWire, 10)
					clientMessages := make(chan protocol.MessageOnTheWire, 10)
					events := make(chan protocol.Event, 10)

					me := peerAddr

					go startServer(ctx, me.NetworkAddress().String(), serverMessages)
					go startClient(ctx, clientMessages)

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
				time.Sleep(5 * time.Second)

				for _, peer := range peers {
					val, err := peer.NumPeers(ctx)
					Expect(err).Should(BeNil())
					Expect(val).Should(Equal(nodeCount.TotalBootstrap + nodeCount.NewNodes))
				}
			})
		}
	})

	Context("when casting", func() {

	})
})
