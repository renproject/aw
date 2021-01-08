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
	Context("when trying to sync valid content id on demand with nil hint", func() {
		It("should successfully receive corresponding message", func() {

			n := 2
			_, peers, tables, contentResolvers, _, _ := setup(n)

			tables[0].AddPeer(peers[1].ID(), wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333+1)), uint64(time.Now().UnixNano())))
			tables[1].AddPeer(peers[0].ID(), wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333)), uint64(time.Now().UnixNano())))

			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)
			}

			helloMsg := "Hi from peer 0"
			contentID := id.NewHash([]byte(helloMsg))
			contentResolvers[0].InsertContent(contentID[:], []byte(helloMsg))

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			msg, err := peers[1].Sync(ctx, contentID[:], nil)

			Ω(err).To(BeNil())
			Ω(msg).To(Equal([]byte(helloMsg)))
		})
	})
})
