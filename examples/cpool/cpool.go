package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/aw/experiment/peer"
	"github.com/renproject/aw/experiment/policy"
	"github.com/renproject/aw/experiment/tcp"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		privKey := id.NewPrivKey()
		self := id.NewSignatory(privKey.PubKey())
		tcp.Dial(context.TODO(), "localhost:3333", func(conn net.Conn) {
			enc, _, _, err := handshake.InsecureHandshake(self)(conn, peer.DefaultEncoder, peer.DefaultDecoder)
			if err != nil {
				panic(err)
			}
			msg := peer.Message{
				Content: []byte("hello, Rahul!"),
			}
			data, err := surge.ToBinary(msg)
			if err != nil {
				panic(err)
			}
			if _, err := enc(conn, data); err != nil {
				panic(err)
			}
			<-time.After(time.Hour)
		}, func(err error) {
			println("error", err)
		}, policy.ConstantTimeout(time.Minute))
	}()

	p := peer.New(
		peer.DefaultOptions().WithCallbacks(peer.Callbacks{
			DidReceiveMessage: func(remote id.Signatory, msg peer.Message) {
				fmt.Printf("received message from %v: %v\n", remote, string(msg.Content))
			},
		}),
		peer.NewInMemTable())
	p.Run(ctx)
}
