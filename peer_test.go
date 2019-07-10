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
			sender := make(chan protocol.MessageOnTheWire, 10)
			receiver := make(chan protocol.MessageOnTheWire, 10)
			bootstrapAddrs := utils.Remove(peerAddresses, i)

			go startServer(ctx, peerAddresses[i].NetworkAddress().String(), receiver)
			go startClient(ctx, sender)

			logger := logrus.StandardLogger()
			if i != 0 {
				logger.SetOutput(ioutil.Discard)
			}
			peer, events := New(PeerOptions{
				Me:                 peerAddresses[i],
				BootstrapAddresses: bootstrapAddrs,
				Codec:              codec,

				Logger:         logger,
				BootstrapDelay: time.Minute,
			}, sender, receiver)

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
		Total int
		Known int
	}{
		// When all the nodes are known
		{4, 4},
		{10, 10},
		{20, 20},
		{40, 40},

		// When half of nodes are known
		{4, 2},
		{10, 5},
		{20, 10},
		{40, 20},

		// When one node is known
		{4, 1},
		{10, 1},
		{20, 1},
		{40, 1},
	}

	Context("when bootstrapping", func() {
		for _, nodeCount := range tableNodeCount {
			It(fmt.Sprintf("should connect to %d nodes when %d nodes are known", nodeCount.Total, nodeCount.Known), func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				bootstrapAddrs, err := startNodes(ctx, nodeCount.Total)
				Expect(err).Should(BeNil())
				sender := make(chan protocol.MessageOnTheWire, 10)
				receiver := make(chan protocol.MessageOnTheWire, 10)
				me := utils.NewSimpleTCPPeerAddress("test_node", "127.0.0.1", fmt.Sprintf("%d", 5000))
				codec := utils.NewSimpleTCPPeerAddressCodec()

				go startServer(ctx, me.NetworkAddress().String(), receiver)
				go startClient(ctx, sender)

				peer, events := New(PeerOptions{
					Me:                 me,
					BootstrapAddresses: bootstrapAddrs[:nodeCount.Known],
					Codec:              codec,

					BootstrapDelay: time.Second,
					Logger:         logrus.StandardLogger(),
				}, sender, receiver)

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

				// wait for the node to bootstrap
				time.Sleep(5 * time.Second)

				val, err := peer.NumPeers(ctx)
				Expect(err).Should(BeNil())
				Expect(val).Should(Equal(nodeCount.Total))
			})
		}
	})
})
