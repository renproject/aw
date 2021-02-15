package peer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func testPeerDiscovery(n int, peers []*peer.Peer, tables []dht.Table, transports []*transport.Transport) context.CancelFunc {
	time.Sleep(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

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

	return cancel
}

func createRingTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table, transports []*transport.Transport) context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for i := range peers {
		go peers[i].Run(ctx)
		tables[i].AddPeer(opts[(i+1)%n].PrivKey.Signatory(),
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333+((i+1)%n))), uint64(time.Now().UnixNano())))
	}
	return cancel
}

func createLineTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table, transports []*transport.Transport) context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for i := range peers {
		go peers[i].Run(ctx)
		if i < n-1 {
			tables[i].AddPeer(opts[i+1].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(3333+i+1)), uint64(time.Now().UnixNano())))

		}
	}
	return cancel
}

func createStarTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table, transports []*transport.Transport) context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for i := range peers {
		go peers[i].Run(ctx)
		if i != 0 {
			tables[i].AddPeer(opts[0].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(3333)), uint64(time.Now().UnixNano())))

		}
	}
	return cancel
}

var _ = Describe("Peer Discovery", func() {
	Context("when trying to discover other peers using the peer discovery client in a ring topology", func() {
		It("should successfully find all peers", func() {
			n := 5
			opts, peers, tables, _, _, transports := setup(n)

			cancelPeerContext := createRingTopology(n, opts, peers, tables, transports)
			defer cancelPeerContext()

			cancelPeerDiscoveryContext := testPeerDiscovery(n, peers, tables, transports)
			defer cancelPeerDiscoveryContext()
		})
	})

	Context("when trying to discover other peers using the peer discovery client in a line topology", func() {
		It("should successfully find all peers", func() {
			n := 5
			opts, peers, tables, _, _, transports := setup(n)

			cancelPeerContext := createLineTopology(n, opts, peers, tables, transports)
			defer cancelPeerContext()

			cancelPeerDiscoveryContext := testPeerDiscovery(n, peers, tables, transports)
			defer cancelPeerDiscoveryContext()
		})
	})

	Context("when trying to discover other peers using the peer discovery client in a star topology", func() {
		It("should successfully find all peers", func() {
			n := 5
			opts, peers, tables, _, _, transports := setup(n)

			cancelPeerContext := createStarTopology(n, opts, peers, tables, transports)
			defer cancelPeerContext()

			cancelPeerDiscoveryContext := testPeerDiscovery(n, peers, tables, transports)
			defer cancelPeerDiscoveryContext()
		})
	})
})
