package gossip_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/gossip"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossip", func() {
	rand.Seed(GinkgoRandomSeed())

	Context("when gossiping data to a peer", func() {
		It("should be received by the node", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			n := uint(rand.Intn(10) + 1)
			nodes := initNodes(ctx, n, int(n))
			f := func(fromIndex, toIndex uint, hash id.Hash, dataType uint8) bool {
				// Restrict variables to a valid range.
				fromIndex = fromIndex % n
				toIndex = toIndex % n

				if fromIndex == toIndex {
					// Prevent sending a message to ourself.
					return true
				}

				// Gossip to a random peer.
				nodes[fromIndex].gossiper.Gossip(id.Hash(nodes[toIndex].signatory), hash, dataType)

				// Ensure the peer receives the data.
				Eventually(func() bool {
					return nodes[toIndex].dht.HasContent(hash, dataType)
				}).Should(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when gossiping data to a subnet", func() {
		It("should be received by the subnet", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			n := uint(rand.Intn(10) + 1)
			nodes := initNodes(ctx, n, int(n))
			f := func(fromIndex, subnetSize uint, hash id.Hash, dataType uint8) bool {
				// Restrict variables to a valid range.
				fromIndex = fromIndex % n
				subnetSize = subnetSize % n

				// Create a random subnet and insert it into the DHT.
				nodeIndices := rand.Perm(int(subnetSize))
				signatories := make([]id.Signatory, subnetSize)
				for i, index := range nodeIndices {
					signatories[i] = nodes[index].signatory
				}
				subnet := nodes[fromIndex].dht.AddSubnet(signatories)

				// Gossip to the subnet.
				nodes[fromIndex].gossiper.Gossip(subnet, hash, dataType)

				// Ensure the subnet receives the data.
				Eventually(func() bool {
					received := true
					for _, index := range nodeIndices {
						if uint(index) == fromIndex {
							// Continue as we do not gossip messages to ourself.
							continue
						}
						received = received && nodes[index].dht.HasContent(hash, dataType)
					}
					return received
				}).Should(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when gossiping data to the default subnet", func() {
		It("should be received by the subnet", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			n := uint(rand.Intn(10) + 1)
			alpha := rand.Intn(int(n))
			nodes := initNodes(ctx, n, alpha)
			f := func(fromIndex, toIndex uint, hash id.Hash, dataType uint8) bool {
				// Restrict variables to a valid range.
				fromIndex = fromIndex % n
				toIndex = toIndex % n

				// Gossip to the default subnet.
				nodes[fromIndex].gossiper.Gossip(gossip.DefaultSubnet, hash, dataType)

				// We expect at least alpha nodes to receive the data.
				Eventually(func() bool {
					numReceived := 0
					for i := range nodes {
						if nodes[i].dht.HasContent(hash, dataType) {
							numReceived++
						}
					}
					return numReceived >= alpha
				}).Should(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when gossiping data to an unknown target", func() {
		It("should not panic", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			n := uint(rand.Intn(10) + 1)
			nodes := initNodes(ctx, n, rand.Intn(100))
			f := func(target, hash id.Hash, dataType uint8) bool {
				// Gossip to a random target and ensure the function does not
				// panic.
				Expect(func() {
					nodes[0].gossiper.Gossip(target, hash, dataType)
				}).ToNot(Panic())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})
})

type node struct {
	privKey   *id.PrivKey
	signatory id.Signatory
	host      string
	port      uint16

	gossiper *gossip.Gossiper
	dht      dht.DHT
}

func initNodes(ctx context.Context, n uint, alpha int) []node {
	nodes := make([]node, n)
	for i := range nodes {
		privKey := id.NewPrivKey()
		signatory := id.NewSignatory(&privKey.PublicKey)
		host := "0.0.0.0"
		port := uint16(3000 + rand.Int()%3000)

		dht := dht.New(
			signatory,
			dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions(),
				nil,
			),
		)
		trans := transport.New(
			transport.DefaultOptions().
				WithTCPServerOptions(
					tcp.DefaultServerOptions().
						WithHost(host).
						WithPort(port),
				),
			handshake.NewInsecure(
				handshake.DefaultOptions(),
			),
		)
		gossiper := gossip.New(
			gossip.DefaultOptions().
				WithAlpha(int(n)),
			signatory,
			dht,
			trans,
		)

		go trans.Run(ctx)
		go gossiper.Run(ctx)

		nodes[i] = node{privKey, signatory, host, port, gossiper, dht}
	}

	// Insert peer addresses into the DHT.
	for i := range nodes {
		for j := range nodes {
			addr := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", nodes[j].host, nodes[j].port), uint64(time.Now().UnixNano()))
			err := addr.Sign(nodes[j].privKey)
			Expect(err).ToNot(HaveOccurred())
			nodes[i].dht.InsertAddr(addr)
		}
	}

	return nodes
}
