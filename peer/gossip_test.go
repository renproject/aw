package peer_test

import (
	"context"
	"fmt"
	rand "math/rand"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var _ = Describe("Gossip", func() {
	Context("When a node is gossipping with peers", func() {
		It("should sync content correctly", func(){
			loggerConfig := zap.NewProductionConfig()
			loggerConfig.Level.SetLevel(zap.PanicLevel)
			logger, err := loggerConfig.Build()
			if err != nil {
				panic(err)
			}

			// Number of peers.
			n := 4

			// Init options for all peers.
			opts := make([]peer.Options, n)
			for i := range opts {
				i := i
				opts[i] = peer.DefaultOptions().WithLogger(logger)
			}

			peers := make([]*peer.Peer, n)
			tables := make([]dht.Table, n)
			contentResolvers := make([]dht.ContentResolver, n)
			clients := make([]*channel.Client, n)
			transports := make([]*transport.Transport, n)
			for i := range peers {
				self := opts[i].PrivKey.Signatory()
				r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
				h := handshake.Filter(func(id.Signatory) error { return nil }, handshake.ECIES(opts[i].PrivKey, r))
				clients[i] = channel.NewClient(
					channel.DefaultOptions().
						WithLogger(logger),
					self)
				tables[i] = dht.NewInMemTable(self)
				contentResolvers[i] = dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
				transports[i] = transport.New(
					transport.DefaultOptions().
						WithLogger(logger).
						WithClientTimeout(5*time.Second).
						WithOncePoolOptions(handshake.DefaultOncePoolOptions().WithMinimumExpiryAge(10*time.Second)).
						WithPort(uint16(3333+i)),
					self,
					clients[i],
					h,
					tables[i])
				peers[i] = peer.New(
					opts[i],
					transports[i],
					contentResolvers[i])
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
				ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
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
				contentResolvers[i].Insert(contentID[:], []byte(msgHello))
				ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
				defer cancel()
				peers[i].Gossip(ctx, contentID[:], &peer.DefaultSubnet)
			}
			<-time.After(5*time.Second)
		})
	})
})
