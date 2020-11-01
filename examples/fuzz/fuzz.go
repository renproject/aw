package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/renproject/aw/experiment/channel"
	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/aw/experiment/peer"
	"github.com/renproject/aw/experiment/transport"
	"github.com/renproject/aw/experiment/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

func main() {
	// Number of peers.
	n := 10

	// Init options for all peers.
	opts := make([]peer.Options, n)
	for i := range opts {
		i := i
		opts[i] = peer.DefaultOptions().WithCallbacks(peer.Callbacks{
			DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
				fmt.Printf("%4v: received \"%v\" from %4v\n", opts[i].PrivKey.Signatory(), string(msg.Data), from)
			},
		})
	}

	// Init and run peers.
	peers := make([]*peer.Peer, n)
	tables := make([]peer.Table, n)
	clients := make([]channel.Client, n)
	transports := make([]*transport.Transport, n)
	for i := range peers {
		loggerConfig := zap.NewProductionConfig()
		loggerConfig.Level.SetLevel(zap.InfoLevel)
		logger, err := loggerConfig.Build()
		if err != nil {
			panic(err)
		}
		self := opts[i].PrivKey.Signatory()
		clients[i] = channel.NewClient(channel.DefaultClientOptions(), self)
		transports[i] = transport.New(transport.DefaultOptions().WithLogger(logger).WithPort(uint16(3333+i)), self, clients[i], handshake.Insecure(self))
		tables[i] = peer.NewInMemTable()
		peers[i] = peer.New(opts[i], tables[i], transports[i])
		go func(i int) {
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
			for {
				// Randomly crash peers.
				func() {
					d := time.Millisecond * time.Duration(1000+r.Int()%9000)
					ctx, cancel := context.WithTimeout(context.Background(), d)
					defer cancel()

					peers[i].Run(ctx)
				}()
			}
		}(i)
	}
	time.Sleep(time.Second)

	for {
		time.Sleep(time.Millisecond * time.Duration(rand.Int()%1000))
		for i := range peers {
			j := (i + 1) % len(peers)
			peers[i].Table().AddPeer(peers[j].ID(), fmt.Sprintf("localhost:%v", 3333+int64(j)))
			if err := peers[i].Send(context.Background(), peers[j].ID(), wire.Msg{Data: []byte(fmt.Sprintf("hello from %v!", i))}); err != nil {
				log.Printf("send: %v", err)
			}
		}
	}

	<-(chan struct{})(nil)
}
