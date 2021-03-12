package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/dht/dhtutil"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Options struct {
	ClientTimeout       int
	TransportTimeout    int
	GossiperOptsTimeout int
	GossiperTimeout     int
	// GossiperWiggleTimeout int
	SyncerTimeout       int
	SyncerWiggleTimeout int
	OncePoolTimeout     int
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	numPeers := 2
	testOpts := Options{
		ClientTimeout:       10,
		TransportTimeout:    1000,
		GossiperOptsTimeout: 10,
		GossiperTimeout:     10,
		// GossiperWiggleTimeout: ,
		SyncerTimeout:       10,
		SyncerWiggleTimeout: 10,
		OncePoolTimeout:     10,
	}

	opts, peers, tables, contentResolvers, _, transports := setup(numPeers, testOpts)

	for i := range opts {
		peers[i].Resolve(context.Background(), contentResolvers[i])
	}

	for i := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), duration(testOpts.TransportTimeout))
		defer cancel()

		index := i
		go func() {
			transports[index].Receive(context.Background(), func() func(from id.Signatory, msg wire.Msg) error {
				var x int64 = 0
				go func() {
					//var seconds int64 = 0
					ticker := time.NewTicker(time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							//seconds += 1
							//fmt.Printf("Average throughput for peer %v: %v/second\n", index, x/seconds)
							atomic.StoreInt64(&x, 0)
						}
					}

				}()
				return func(from id.Signatory, msg wire.Msg) error {
					atomic.AddInt64(&x, 1)
					if err := peers[index].Syncer().DidReceiveMessage(from, msg); err != nil {
						return err
					}
					if err := peers[index].Gossiper().DidReceiveMessage(from, msg); err != nil {
						return err
					}
					return nil
				}
			}())
			transports[index].Run(ctx)
		}()
		for j := range peers {
			if i != j {
				transports[i].Link(peers[j].ID())
			}
		}

		tables[i].AddPeer(opts[(i+1)%numPeers].PrivKey.Signatory(),
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333+i+1)), uint64(time.Now().UnixNano())))
		tables[(i+1)%numPeers].AddPeer(opts[i].PrivKey.Signatory(),
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333+i)), uint64(time.Now().UnixNano())))
	}

	timeout := duration(testOpts.GossiperTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	for iter := 0; iter < 1; iter++ {
		select {
		case <-ctx.Done():
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
		default:
		}
		for i := range peers {
			msgHello := fmt.Sprintf(string(dhtutil.RandomContent()), peers[i].ID().String())
			contentID := id.NewHash([]byte(msgHello))
			contentResolvers[i].InsertContent(contentID[:], []byte(msgHello))
			peers[i].Gossip(ctx, contentID[:], &peer.DefaultSubnet)
		}
		fmt.Println("Round", iter)
	}
	cancel()

	time.Sleep(5 * time.Second)
	fmt.Printf("%v\n", testOpts)
}

func duration(num int) time.Duration {
	return time.Duration(num) * time.Second
}

func setup(numPeers int, testOpts Options) ([]peer.Options, []*peer.Peer, []dht.Table, []dht.ContentResolver, []*channel.Client, []*transport.Transport) {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.DebugLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	// Init options for all peers.
	opts := make([]peer.Options, numPeers)
	for i := range opts {
		i := i
		opts[i] = peer.DefaultOptions().WithLogger(logger)

		opts[i].GossiperOptions.Timeout = duration(testOpts.GossiperOptsTimeout)
		// opts[i].GossiperOptions.WiggleTimeout = duration(testOpts.GossiperWiggleTimeout)

		opts[i].SyncerOptions.WiggleTimeout = duration(testOpts.SyncerWiggleTimeout)
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
				WithClientTimeout(duration(testOpts.ClientTimeout)).
				WithOncePoolOptions(handshake.DefaultOncePoolOptions().WithMinimumExpiryAge(duration(testOpts.OncePoolTimeout))).
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
