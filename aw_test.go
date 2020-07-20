package aw_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/renproject/aw"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Airwave", func() {
	Context("when one node is connected to another", func() {
		Context("when one node sends a message to another", func() {
			It("should deliver the message", func() {
				defer time.Sleep(time.Millisecond)

				willSendN := uint64(10)
				didReceiveN := uint64(0)
				didReceiveOnce := false
				didReceiveDone := false

				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				port1 := uint16(3000 + r.Int()%3000)
				node1 := aw.New().
					WithAddr(wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("0.0.0.0:%v", port1), uint64(time.Now().UnixNano()))).
					WithHost("0.0.0.0").
					WithPort(port1).
					Build()

				port2 := uint16(3000 + r.Int()%3000)
				node2 := aw.New().
					WithAddr(wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("0.0.0.0:%v", port2), uint64(time.Now().UnixNano()))).
					WithHost("0.0.0.0").
					WithPort(port2).
					WithContentResolver(
						dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), dht.CallbackContentResolver{
							InsertCallback: func(hash id.Hash, contentType uint8, content []byte) {
								defer GinkgoRecover()
								if len(content) == 0 {
									// Ignore empty content.
									return
								}
								if string(content) == "once" {
									Expect(didReceiveOnce).To(BeFalse())
									didReceiveOnce = true
									return
								}
								if string(content) == "done" {
									Expect(didReceiveDone).To(BeFalse())
									didReceiveDone = true
									cancel()
									return
								}
								atomic.AddUint64(&didReceiveN, 1)
							},
						}),
					).
					Build()

				node1.DHT().InsertAddr(node2.Addr())

				go node1.Run(ctx)
				go node2.Run(ctx)

				// Sleep for enough time for nodes to find each other by pinging
				// each other.
				time.Sleep(100 * time.Millisecond)

				subnet := node1.DHT().AddSubnet([]id.Signatory{node2.Identity()})
				fmt.Printf("%v\n", subnet)
				for i := uint64(0); i < willSendN; i++ {
					node1.Broadcast(ctx, subnet, 0, []byte("once"))
					node1.Broadcast(ctx, subnet, 0, []byte(fmt.Sprintf("message #%v", i)))
				}
				node1.Broadcast(ctx, subnet, 0, []byte("done"))

				<-ctx.Done()

				Expect(didReceiveN).To(Equal(willSendN))
				Expect(didReceiveOnce).To(BeTrue())
				Expect(didReceiveDone).To(BeTrue())
			})
		})
	})

	Context("when gossiping", func() {
		Context("when fully connected", func() {
			FIt("should return content from all nodes", func() {
				defer time.Sleep(time.Millisecond)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				// Initialise nodes.
				n := 3
				nodes := make([]*aw.Node, n)
				for i := range nodes {
					port := uint16(3000 + i)
					node := aw.New().
						WithAddr(wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("0.0.0.0:%v", port), uint64(time.Now().UnixNano()))).
						WithHost("0.0.0.0").
						WithPort(port).
						Build()
					nodes[i] = node
				}

				// Connect nodes in a fully connected cyclic graph.
				for i := range nodes {
					for j := range nodes {
						if i == j {
							continue
						}
						nodes[i].DHT().InsertAddr(nodes[j].Addr())
					}
				}

				// Run the nodes.
				for i := range nodes {
					go nodes[i].Run(ctx)
				}

				// Sleep for enough time for nodes to find each other by pinging
				// each other.
				time.Sleep(100 * time.Millisecond)

				contentHash := sha256.Sum256([]byte("hello!"))
				contentType := uint8(1)
				content := []byte("hello!")
				// nodes[0].Broadcast(ctx, gossip.DefaultSubnet, contentType, content)

				found := map[id.Signatory]struct{}{}
				for {
					time.Sleep(time.Millisecond)
					for i := range nodes {
						data, ok := nodes[i].DHT().Content(contentHash, contentType)
						if !ok {
							continue
						}
						if bytes.Equal(content, data) {
							found[nodes[i].Identity()] = struct{}{}
						}
					}
					if len(found) == n {
						return
					}
				}
			})
		})
	})
})
