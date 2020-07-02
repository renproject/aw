package gossip_test

import (
	"context"
	"fmt"
	"log"
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
	Context("when gossiping data to a peer", func() {
		It("should be received by the node", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			numNodes := uint(5)
			nodes, _ := initNodes(ctx, numNodes)
			f := func(fromIndex, toIndex uint, hash id.Hash, dataType uint8) bool {
				// Restrict indices to a valid range.
				fromIndex = fromIndex % numNodes
				toIndex = toIndex % numNodes

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

			numNodes := uint(10)
			nodes, subnets := initNodes(ctx, numNodes)
			f := func(fromIndex, toIndex uint, hash id.Hash, dataType uint8) bool {
				// Restrict indices to a valid range.
				fromIndex = fromIndex % numNodes
				toIndex = toIndex % numNodes

				// Gossip to a random peer.
				nodes[fromIndex].gossiper.Gossip(subnets[fromIndex], hash, dataType)

				// Ensure the subnet receives the data.
				Eventually(func() bool {
					received := true
					for i := range nodes {
						if uint(i) == fromIndex {
							continue
						}
						received = received && nodes[i].dht.HasContent(hash, dataType)
					}
					return received
				}, 3*time.Second).Should(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when gossiping data to the default subnet", func() {
		It("should be received by the subnet", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			numNodes := uint(10)
			nodes, subnets := initNodes(ctx, numNodes)
			f := func(fromIndex, toIndex uint, hash id.Hash, dataType uint8) bool {
				// Restrict indices to a valid range.
				fromIndex = fromIndex % numNodes
				toIndex = toIndex % numNodes

				// Gossip to a random peer.
				nodes[fromIndex].gossiper.Gossip(subnets[fromIndex], hash, dataType)

				// Ensure the subnet receives the data.
				Eventually(func() bool {
					received := true
					for i := range nodes {
						if uint(i) == fromIndex {
							continue
						}
						received = received && nodes[i].dht.HasContent(hash, dataType)
					}
					return received
				}, 3*time.Second).Should(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when gossiping data to an unknown target", func() {
		It("should not panic", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			nodes, _ := initNodes(ctx, 5)
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

func initNodes(ctx context.Context, n uint) ([]node, []id.Hash) {
	nodes := make([]node, n)
	signatories := make([]id.Signatory, n)
	for i := range nodes {
		privKey := id.NewPrivKey()
		signatories[i] = id.NewSignatory(&privKey.PublicKey)
		host := "0.0.0.0"
		port := uint16(3000 + rand.Int()%3000)

		dht := dht.New(
			signatories[i],
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
				WithAlpha(int(n)).
				WithBias(1),
			signatories[i],
			dht,
			trans,
		)

		go trans.Run(ctx)
		go gossiper.Run(ctx)

		nodes[i] = node{privKey, signatories[i], host, port, gossiper, dht}
	}

	subnets := make([]id.Hash, n)
	for i := range nodes {
		// Insert peer addresses into the DHT.
		for j := range nodes {
			addr := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", nodes[j].host, nodes[j].port), uint64(time.Now().UnixNano()))
			err := addr.Sign(nodes[j].privKey)
			Expect(err).ToNot(HaveOccurred())
			nodes[i].dht.InsertAddr(addr)
		}

		// Insert the subnet into the DHT.
		log.Println(len(signatories))
		subnets[i] = nodes[i].dht.AddSubnet(signatories)
	}

	return nodes, subnets
}
