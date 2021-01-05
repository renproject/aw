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
				peers[i].Receive(context.Background(), func(from id.Signatory, msg wire.Msg) error {return nil})
			}
			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
				defer cancel()
				go peers[i].Run(ctx)
				tables[i].AddPeer(opts[(i+1)%n].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(3333+((i+1)%n))), uint64(time.Now().UnixNano())))
			}

			<-time.After(1 * time.Second)
			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
				defer cancel()
				peers[i].PeerDiscovery(ctx)
			}
			<-time.After(5 * time.Second)

			for i := range peers {
				for j := range peers {
					if i != j {
						addr, ok := tables[i].PeerAddress(transports[j].Self())
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
