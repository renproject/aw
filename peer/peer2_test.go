package peer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("Peer2", func() {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.DebugLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	It("", func() {
		listenerRateLimiterOptions := peer.RateLimiterOptions{
			Rate:     1,
			Burst:    10,
			Capacity: 10,
		}
		connectionRateLimiterOptions := peer.RateLimiterOptions{
			Rate:     1 * 1024 * 1024,
			Burst:    1 * 1024 * 1024,
			Capacity: 10,
		}

		opts := peer.Options2{
			Logger: logger,

			MaxLinkedPeers:               2,
			MaxEphemeralConnections:      2,
			MaxPendingSyncs:              1,
			MaxActiveSyncsForSameContent: 1,
			MaxGossipSubnets:             10,
			OutgoingBufferSize:           10,
			EventLoopBufferSize:          10,
			DialRetryInterval:            time.Second,
			EphemeralConnectionTTL:       time.Second,
			MinimumConnectionExpiryAge:   time.Second,

			GossipAlpha:   1,
			GossipTimeout: time.Second,

			PingAlpha:             1,
			PongAlpha:             1,
			PeerDiscoveryInterval: time.Hour,

			ListenerRateLimiterOptions:   listenerRateLimiterOptions,
			ConnectionRateLimiterOptions: connectionRateLimiterOptions,
		}

		privKey1 := id.NewPrivKey()
		privKey2 := id.NewPrivKey()

		peerTable1 := dht.NewInMemTable(privKey1.Signatory())
		peerTable2 := dht.NewInMemTable(privKey2.Signatory())

		contentResolver1 := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
		contentResolver2 := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)

		peer1 := peer.New2(opts, privKey1, peerTable1, contentResolver1)
		peer2 := peer.New2(opts, privKey2, peerTable2, contentResolver2)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		port1, err := peer1.Listen(ctx, "localhost:0")
		if err != nil {
			panic(err)
		}
		port2, err := peer2.Listen(ctx, "localhost:0")
		if err != nil {
			panic(err)
		}

		go peer1.Run(ctx)
		go peer2.Run(ctx)

		addr1 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", port1), uint64(time.Now().UnixNano()))
		addr2 := wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", "localhost", port2), uint64(time.Now().UnixNano()))

		peerTable1.AddPeer(peer2.Self, addr2)
		peerTable2.AddPeer(peer1.Self, addr1)

		contentID := []byte("id")
		data := []byte("hello")
		contentResolver1.InsertContent(contentID, data)

		peer1.Link(peer2.Self)
		peer2.Link(peer1.Self)

		peer1.Gossip(contentID, nil)

		Eventually(func() []byte { msg, _ := contentResolver2.QueryContent(contentID); return msg }, 999999999999).Should(Equal(data))
	})
})
