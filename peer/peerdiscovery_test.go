package peer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Peer", func() {
	Context("when trying to discover other peers using the peer discovery client", func() {
		It("should successfully find all peers", func() {
			n := 5
			opts, peers, tables, _, _, transports := setup(n)

			for i := range peers {
				peers[i].Receive(context.Background(), func(from id.Signatory, msg wire.Msg) error { return nil })
			}
			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)
				tables[i].AddPeer(opts[(i+1)%n].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(3333+((i+1)%n))), uint64(time.Now().UnixNano())))
			}

			time.Sleep(time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := range peers {
				go peers[i].DiscoverPeers(ctx)
			}
			<-ctx.Done()

			for i := range peers {
				Expect(tables[i].NumPeers()).To(Equal(n))
				for j := range peers {
					if i != j {
						self := transports[j].Self()
						addr, ok := tables[i].PeerAddress(transports[j].Self())
						if !ok {
							fmt.Printf("Sig not found: %v\n", self)
							for _, k := range tables[i].Peers(10) {
								sig := id.Signatory{}
								copy(sig[:], k[:])
								x, _ := tables[i].PeerAddress(sig)
								fmt.Printf("Sig in table: %v, Addr: %v\n", sig, x)
							}
						}
						Expect(ok).To(BeTrue())
						Expect(addr.Value).To(Or(
							Equal(fmt.Sprintf("127.0.0.1:%v", uint16(3333+j))),
							Equal(fmt.Sprintf("localhost:%v", uint16(3333+j))),
							Equal(fmt.Sprintf(":%v", uint16(3333+j)))))
					}
				}
			}
		})
	})
})
