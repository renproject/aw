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
			go DefaultTCP(options, events, 46532+i).Run(ctx)
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
				defer time.Sleep(5 * time.Second)

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
					peer := DefaultTCP(PeerOptions{
						Logger: logger.WithField("test_node", i),
						Codec:  codec,

						SignVerifier:       nodeSignVerifiers[i],
						Me:                 peerAddr,
						BootstrapAddresses: bootstrapAddrs,
					}, events, 5000+i)
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
})
