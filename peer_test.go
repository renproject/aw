package aw_test

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/phi"
	"github.com/renproject/phi/co"
	"github.com/sirupsen/logrus"
)

const CAPACITY = 0

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

	startNodes := func(ctx context.Context, logger logrus.FieldLogger, signVerifiers []testutil.MockSignVerifier, nodeCount int, cap int) ([]Peer, PeerAddresses, error) {
		codec := testutil.NewSimpleTCPPeerAddressCodec()
		peerAddresses := make([]PeerAddress, nodeCount)
		peers := make([]Peer, nodeCount)
		for i := range peerAddresses {
			peerAddresses[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("bootstrap_%d", i), "127.0.0.1", fmt.Sprintf("%d", 46532+i))
		}
		co.ParForAll(peerAddresses, func(i int) {
			events := make(chan protocol.Event, cap)
			options := PeerOptions{
				Logger: logger.WithField("bootstrap", i),
				Codec:  codec,

				Me:                 peerAddresses[i],
				BootstrapAddresses: testutil.Remove(peerAddresses, i),
			}
			if signVerifiers != nil && len(signVerifiers) == len(peerAddresses) {
				options.SignVerifier = signVerifiers[i]
			}
			peers[i] = NewTCPPeer(options, events, CAPACITY, 46532+i)
			go peers[i].Run(ctx)
		})
		return peers, peerAddresses, nil
	}

	tableNodeCount := []struct {
		TotalBootstrap int
		KnownBootstrap int

		NewNodes int
	}{
		// When all the nodes are known
		// {4, 4, 4},
		{30, 30, 30},
		// {20, 20, 10},
		// {40, 40, 40},

		// When half of nodes are known
		// {4, 2, 4},
		// {10, 5, 10},
		// {20, 10, 20},
		// {40, 20, 40},

		// When one node is known
		// {4, 1, 4},
		// {10, 1, 10},
		// {20, 1, 20},
		// {40, 1, 40},
	}

	Context("when bootstrapping", func() {
		for _, nodeCount := range tableNodeCount {
			nodeCount := nodeCount
			FIt(fmt.Sprintf(
				"should connect to %d nodes when %d nodes are known, when new nodes join sequentially",
				nodeCount.TotalBootstrap+nodeCount.NewNodes-1, nodeCount.KnownBootstrap), func() {

				rand.Seed(time.Now().UnixNano())

				numMessageSpams := 1
				logger := logrus.StandardLogger()
				SignVerifiers := initSignVerifiers(nodeCount.TotalBootstrap + nodeCount.NewNodes)
				bootstrapSignVerifiers := make([]testutil.MockSignVerifier, nodeCount.TotalBootstrap)
				nodeSignVerifiers := make([]testutil.MockSignVerifier, nodeCount.NewNodes)
				copy(bootstrapSignVerifiers, SignVerifiers[:nodeCount.TotalBootstrap])
				copy(nodeSignVerifiers, SignVerifiers[nodeCount.TotalBootstrap:])

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				bootstraps, bootstrapAddrs, err := startNodes(ctx, logger, bootstrapSignVerifiers, nodeCount.TotalBootstrap, numMessageSpams*len(SignVerifiers)*(len(SignVerifiers)-1))
				Expect(err).Should(BeNil())

				peerAddresses := make([]PeerAddress, nodeCount.NewNodes)
				for i := range peerAddresses {
					peerAddresses[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("test_node_%d", i), "127.0.0.1", fmt.Sprintf("%d", 5000+i))
				}
				codec := testutil.NewSimpleTCPPeerAddressCodec()
				peers := make([]Peer, nodeCount.NewNodes)
				for i, peerAddr := range peerAddresses {
					events := make(chan protocol.Event, numMessageSpams*len(SignVerifiers)*(len(SignVerifiers)-1))
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

				allNodes := append(bootstraps, peers...)
				spam := func(done chan<- struct{}) {
					broadcastCtx, broadcastCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer func() {
						broadcastCancel()
						close(done)
					}()

					phi.ParForAll(allNodes, func(index int) {
						time.Sleep(time.Duration(rand.Intn(10)+300) * time.Millisecond)
						msg := randomBytes(1000)
						err := allNodes[index].Multicast(broadcastCtx, msg)
						Expect(err).ShouldNot(HaveOccurred())
					})
				}

				dones := make([]chan struct{}, numMessageSpams)
				for i := 0; i < numMessageSpams; i++ {
					dones[i] = make(chan struct{})
					go spam(dones[i])
				}
				finished := numMessageSpams
				for finished > 0 {
					for i := 0; i < numMessageSpams; i++ {
						select {
						case <-dones[i]:
							finished--
						default:
						}
					}
				}

				time.Sleep(time.Minute)

			})
		}
	})
})

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}

	return b
}
