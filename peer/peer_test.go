package peer_test

import (
	"context"
	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/id"
	"go.uber.org/zap"
	"time"
)

func setup(numPeers int) ([]peer.Options, []*peer.Peer, []dht.Table, []dht.ContentResolver, []*channel.Client, []*transport.Transport) {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.PanicLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	// Init options for all peers.
	opts := make([]peer.Options, numPeers)
	for i := range opts {
		i := i
		opts[i] = peer.DefaultOptions().WithLogger(logger)
	}

	peers := make([]*peer.Peer, numPeers)
	tables := make([]dht.Table, numPeers)
	contentResolvers := make([]dht.ContentResolver, numPeers)
	clients := make([]*channel.Client, numPeers)
	transports := make([]*transport.Transport, numPeers)
	for i := range peers {
		self := opts[i].PrivKey.Signatory()
		h := handshake.Filter(func(id.Signatory) error { return nil }, handshake.ECIES(opts[i].PrivKey))
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
			transports[i])
		peers[i].Resolve(context.Background(), contentResolvers[i])
	}
	return opts, peers, tables, contentResolvers, clients, transports
}
