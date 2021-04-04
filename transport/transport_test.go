package transport_test

import (
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/transport"
	"github.com/renproject/id"

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
		self := privKeys[i].Signatory()
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
