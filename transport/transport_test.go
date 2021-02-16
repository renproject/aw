package transport_test

import (
	"context"
	"fmt"
	"github.com/renproject/aw/wire"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/transport"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

func setup(numPeers int) ([]*id.PrivKey, []dht.Table, []*channel.Client, []*transport.Transport) {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.DebugLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	// Init options for all peers.

	privKeys := make([]*id.PrivKey, numPeers)
	tables := make([]dht.Table, numPeers)
	clients := make([]*channel.Client, numPeers)
	transports := make([]*transport.Transport, numPeers)
	for i := range privKeys {
		privKeys[i] = id.NewPrivKey()
		self :=  privKeys[i].Signatory()
		h := handshake.Filter(func(id.Signatory) error { return nil }, handshake.ECIES(privKeys[i]))
		clients[i] = channel.NewClient(
			channel.DefaultOptions().
				WithLogger(logger),
			self)
		tables[i] = dht.NewInMemTable(self)
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
	}
	return privKeys, tables, clients, transports
}

var _ = Describe("Transport", func() {
	Context("when starting the transport layer", func() {
		It("a node should register itself with in the hash table", func() {
			privKeys, tables, _, transports := setup(1)
			go transports[0].Run(context.Background())
			time.Sleep(500 * time.Millisecond)
			addr, ok := tables[0].PeerAddress(privKeys[0].Signatory())
			Expect(ok).To(BeTrue())
			Expect(addr.Value).To(Or(
				Equal(fmt.Sprintf("127.0.0.1:%v", uint16(3333))),
				Equal(fmt.Sprintf("localhost:%v", uint16(3333))),
				Equal(fmt.Sprintf(":%v", uint16(3333)))))
		})
	})

	Context("when a transport dial is successful in establishing a connection", func() {
		It("the IP address of the dialer node should be registered in the hash table", func() {
			_, tables, _, transports := setup(2)
			ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
			defer cancel()
			transports[1].Receive(ctx, func(from id.Signatory, msg wire.Msg) error {return nil})
			go transports[1].Run(ctx)
			println("Receive finished")
			tables[0].AddPeer(transports[1].Self(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(3333+1)), uint64(time.Now().UnixNano())))
			time.Sleep(500 * time.Millisecond)

			msg := []byte("Hello")
			sig := transports[1].Self()
			err := transports[0].Send(ctx, transports[1].Self(), wire.Msg{
				Type: wire.MsgTypeSend,
				To: id.Hash(sig),
				Data: msg,
				})
			Expect(err).To(BeNil())
			Expect(tables[0].IP(transports[1].Self())).To(Or(
				Equal(fmt.Sprintf("127.0.0.1")),
				Equal(fmt.Sprintf("localhost")),
				Equal(fmt.Sprintf(""))))
		})
	})
})
