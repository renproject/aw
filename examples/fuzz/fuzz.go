package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

func main() {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.PanicLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	// Number of peers.
	n := 10

	// Init options for all peers.
	opts := make([]peer.Options, n)
	for i := range opts {
		i := i
		opts[i] = peer.DefaultOptions().WithLogger(logger).WithCallbacks(peer.Callbacks{
			DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
				fmt.Printf("%4v: received \"%v\" from %4v\n", opts[i].PrivKey.Signatory(), string(msg.Data), from)
			},
		})
	}

	// Init and run peers.
	peers := make([]*peer.Peer, n)
	tables := make([]peer.Table, n)
	clients := make([]*channel.Client, n)
	transports := make([]*transport.Transport, n)
	for i := range peers {
		self := opts[i].PrivKey.Signatory()
		clients[i] = channel.NewClient(
			channel.DefaultClientOptions().
				WithLogger(logger),
			self)
		transports[i] = transport.New(
			transport.DefaultOptions().
				WithLogger(logger).
				WithPort(uint16(3333+i)),
			self,
			clients[i],
			handshake.Insecure(opts[i].PrivKey.Signatory()))
		tables[i] = peer.NewInMemTable()
		peers[i] = peer.New(opts[i], tables[i], transports[i])
		go func(i int) {
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
			for {
				// Randomly crash peers.
				func() {
					d := time.Minute * time.Duration(1000+r.Int()%9000)
					ctx, cancel := context.WithTimeout(context.Background(), d)
					defer cancel()
					peers[i].Run(ctx)
				}()
			}
		}(i)
	}

	for {
		time.Sleep(time.Millisecond * time.Duration(rand.Int()%1000))
		for i := range peers {
			j := (i + 1) % len(peers)
			fmt.Printf("peer[%v] sending to peer[%v]\n", i, j)
			peers[i].Table().AddPeer(peers[j].ID(), fmt.Sprintf("localhost:%v", 3333+int64(j)))
			if err := peers[i].Send(context.Background(), peers[j].ID(), wire.Msg{Data: []byte(fmt.Sprintf("hello from %v!", i))}); err != nil {
				log.Printf("send: %v", err)
			}
		}
	}
}
