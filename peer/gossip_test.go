package peer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossip", func() {
	Context("When a node is gossipping with peers", func() {
		It("should sync content correctly", func() {

			// Number of peers
			n := 4
			opts, peers, tables, contentResolvers, _, _ := setup(n)

			for i := range peers {
				self := peers[i].ID()
				peers[i].Receive(context.Background(), func(from id.Signatory, msg wire.Msg) error {
					switch msg.Type {
					case wire.MsgTypePush:
						fmt.Printf("%v received Push from %v\n", self.String(), from.String())
					case wire.MsgTypePull:
						fmt.Printf("%v received Pull from %v\n", self.String(), from.String())
					case wire.MsgTypeSync:
						fmt.Printf("%v received Sync from %v saying : %v\n", self.String(), from.String(), string(msg.SyncData))
					}
					return nil
				})
			}
			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)
				tables[i].AddPeer(opts[(i+1)%n].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(3333+i+1)), uint64(time.Now().UnixNano())))
				tables[(i+1)%n].AddPeer(opts[i].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(3333+i)), uint64(time.Now().UnixNano())))
			}
			for i := range peers {
				msgHello := fmt.Sprintf("Hi from %v", peers[i].ID().String())
				contentID := id.NewHash([]byte(msgHello))
				contentResolvers[i].InsertContent(contentID[:], []byte(msgHello))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peers[i].Gossip(ctx, contentID[:], &peer.DefaultSubnet)
			}
			<-time.After(5 * time.Second)
			for i := range peers {
				for j := range peers {
					msgHello := fmt.Sprintf("Hi from %v", peers[j].ID().String())
					contentID := id.NewHash([]byte(msgHello))
					_, ok := contentResolvers[i].QueryContent(contentID[:])
					Expect(ok).To(BeTrue())
				}
			}
		})
	})
})
