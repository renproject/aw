package aw_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/renproject/aw"
	"github.com/renproject/aw/gossip"
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
					WithListener(
						gossip.Callbacks{
							ReceiveContent: func(hash id.Hash, contentType uint8, content []byte) {
								defer GinkgoRecover()
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
						},
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
})
