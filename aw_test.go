package aw_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/renproject/aw"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/gossip"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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
				addr1 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("0.0.0.0:%v", port1), uint64(time.Now().UnixNano()))
				privKey1 := id.NewPrivKey()
				Expect(addr1.Sign(privKey1)).To(Succeed())

				tcpClientOpts := tcp.DefaultClientOptions().
					WithTimeToDial(1 * time.Second)
				tcpServerOpts := tcp.DefaultServerOptions().
					WithHost("0.0.0.0").
					WithPreventDuplicateConns(false)

				logger, _ := zap.Config{
					Encoding: "json",
					Level:    zap.NewAtomicLevelAt(zapcore.ErrorLevel),
				}.Build()

				node1 := aw.New().
					WithPrivKey(privKey1).
					WithAddr(addr1).
					WithTCPClientOptions(tcpClientOpts).
					WithTCPServerOptions(tcpServerOpts.WithPort(port1)).
					WithLogger(logger).
					Build()

				port2 := uint16(3000 + r.Int()%3000)
				addr2 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("0.0.0.0:%v", port2), uint64(time.Now().UnixNano()))
				privKey2 := id.NewPrivKey()
				Expect(addr2.Sign(privKey2)).To(Succeed())

				node2 := aw.New().
					WithPrivKey(privKey2).
					WithAddr(addr2).
					WithTCPClientOptions(tcpClientOpts).
					WithTCPServerOptions(tcpServerOpts.WithPort(port2)).
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
					WithLogger(logger).
					Build()

				node1.DHT().InsertAddr(node2.Addr())

				go node1.Run(ctx)
				go node2.Run(ctx)

				// Sleep for enough time for nodes to find each other by pinging
				// each other.
				time.Sleep(100 * time.Millisecond)

				subnet := node1.DHT().AddSubnet([]id.Signatory{node2.Identity()})
				for i := uint64(0); i < willSendN; i++ {
					data1 := []byte("once")
					data2 := []byte(fmt.Sprintf("message #%v", i))
					hash1 := aw.Hash(0, data1)
					hash2 := aw.Hash(0, data2)

					node1.DHT().InsertContent(hash1, 0, data1)
					node1.Broadcast(ctx, subnet, hash1, 0, data1)
					node1.DHT().InsertContent(hash2, 0, data2)
					node1.Broadcast(ctx, subnet, hash2, 0, data2)
				}
				data := []byte("done")
				hash := aw.Hash(0, data)
				node1.DHT().InsertContent(hash, 0, data)
				node1.Broadcast(ctx, subnet, hash, 0, data)

				<-ctx.Done()

				Expect(didReceiveN).To(Equal(willSendN))
				Expect(didReceiveOnce).To(BeTrue())
				Expect(didReceiveDone).To(BeTrue())
			})
		})
	})

	Context("when gossiping", func() {
		Context("when fully connected", func() {
			It("should return content from all nodes", func() {
				defer time.Sleep(time.Millisecond)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				// Initialise nodes.
				n := 3
				nodes := make([]*aw.Node, n)
				addrs := make([]wire.Address, n)

				tcpClientOpts := tcp.DefaultClientOptions().
					WithTimeToDial(1 * time.Second)
				tcpServerOpts := tcp.DefaultServerOptions().
					WithHost("0.0.0.0").
					WithPreventDuplicateConns(false)

				logger, _ := zap.Config{
					Encoding: "json",
					Level:    zap.NewAtomicLevelAt(zapcore.ErrorLevel),
				}.Build()

				for i := range nodes {
					port := uint16(3000 + i)
					addrs[i] = wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("0.0.0.0:%v", port), uint64(time.Now().UnixNano()))
					privKey := id.NewPrivKey()
					Expect(addrs[i].Sign(privKey)).To(Succeed())

					node := aw.New().
						WithPrivKey(privKey).
						WithAddr(addrs[i]).
						WithTCPClientOptions(tcpClientOpts).
						WithTCPServerOptions(tcpServerOpts.WithPort(port)).
						WithLogger(logger).
						Build()

					nodes[i] = node
				}

				// Connect nodes in a fully connected cyclic graph.
				for i := range nodes {
					for j := range nodes {
						if i == j {
							continue
						}
						nodes[i].DHT().InsertAddr(addrs[j])
					}
				}

				// Run the nodes.
				for i := range nodes {
					go nodes[i].Run(ctx)
				}

				// Sleep for enough time for nodes to find each other by pinging
				// each other.
				time.Sleep(100 * time.Millisecond)

				content := []byte("hello!")
				contentType := uint8(1)
				contentHash := aw.Hash(contentType, content)
				nodes[0].DHT().InsertContent(contentHash, contentType, content)
				nodes[0].Broadcast(ctx, gossip.DefaultSubnet, contentHash, contentType, content)

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
