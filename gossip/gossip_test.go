package gossip_test

import (
	"bytes"
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

	Context("when syncing data from a subnet", func() {
		Context("if the data exists", func() {
			It("should receive the data", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				n := uint(rand.Intn(6) + 5)
				nodes := initNodes(ctx, n, int(n))
				f := func(syncIndex, subnetSize uint, hash id.Hash, dataType uint8, data []byte) bool {
					// Restrict variables to a valid range.
					syncIndex = syncIndex % n
					subnetSize = subnetSize%(n-2) + 2 // Ensure the subnet has at least 2 members.

					// Create a random subnet and insert it into the DHT.
					nodeIndices := rand.Perm(int(subnetSize))
					signatories := make([]id.Signatory, subnetSize)
					for i, index := range nodeIndices {
						signatories[i] = nodes[index].signatory

						// Insert some data that the subnet knows about.
						nodes[index].dht.InsertContent(hash, dataType, data)
					}
					subnet := nodes[syncIndex].dht.AddSubnet(signatories)

					// Sync data from the subnet and ensure it is the same.
					innerCtx, innerCancel := context.WithTimeout(ctx, 100*time.Millisecond)
					defer innerCancel()

					newData, err := nodes[syncIndex].gossiper.Sync(innerCtx, subnet, hash, dataType)
					Expect(err).ToNot(HaveOccurred())

					return bytes.Equal(newData, data)
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("if the data does not exist", func() {
			It("should receive the data", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				n := uint(rand.Intn(10) + 1)
				nodes := initNodes(ctx, n, int(n))
				f := func(syncIndex, subnetSize uint, hash id.Hash, dataType uint8, data []byte) bool {
					// Restrict variables to a valid range.
					syncIndex = syncIndex % n
					subnetSize = subnetSize % n

					// Create a random subnet and insert it into the DHT.
					nodeIndices := rand.Perm(int(subnetSize))
					signatories := make([]id.Signatory, subnetSize)
					for i, index := range nodeIndices {
						signatories[i] = nodes[index].signatory
					}
					subnet := nodes[syncIndex].dht.AddSubnet(signatories)

					// Sync data with an unknown hash from the subnet and ensure
					// it returns an error.
					innerCtx, innerCancel := context.WithTimeout(ctx, 100*time.Millisecond)
					defer innerCancel()

					_, err := nodes[syncIndex].gossiper.Sync(innerCtx, subnet, hash, dataType)
					return err != nil
				}
				Expect(quick.Check(f, &quick.Config{MaxCount: 20})).To(Succeed())
			})
		})
	})

	Context("when syncing data from the default subnet", func() {
		It("should receive the data", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			n := uint(rand.Intn(9) + 2)
			nodes := initNodes(ctx, n, int(n))
			f := func(syncIndex uint, hash id.Hash, dataType uint8, data []byte) bool {
				// Restrict variables to a valid range.
				syncIndex = syncIndex % n

				// Insert data into all nodes except the one that is expected to
				// sync it.
				for i := range nodes {
					if uint(i) == syncIndex {
						continue
					}
					nodes[i].dht.InsertContent(hash, dataType, data)
				}

				// Sync data from the default subnet and ensure it is the same.
				innerCtx, innerCancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer innerCancel()

				newData, err := nodes[syncIndex].gossiper.Sync(innerCtx, gossip.DefaultSubnet, hash, dataType)
				Expect(err).ToNot(HaveOccurred())

				return bytes.Equal(newData, data)
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
						WithPort(port).
						WithPreventDuplicateConns(false),
				),
			handshake.NewInsecure(
				handshake.DefaultOptions().
					WithPrivKey(privKey),
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
