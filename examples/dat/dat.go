package main

import (
	"context"
	"fmt"
	"time"

	"github.com/renproject/experiment/peer"
	"github.com/renproject/id"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := peer.New(
		peer.DefaultOptions().WithCallbacks(peer.Callbacks{
			DidReceiveMessage: func(remote id.Signatory, msg peer.Message) {
				fmt.Printf("received message from %v: %v\n", remote, string(msg.Content))
			},
		}),
		peer.NewInMemTable())

	go p1.Run(ctx)

	p2 := peer.New(
		peer.DefaultOptions().WithPort(4444).WithCallbacks(peer.Callbacks{
			DidReceiveMessage: func(remote id.Signatory, msg peer.Message) {
				fmt.Printf("received message from %v: %v\n", remote, string(msg.Content))
			},
		}),
		peer.NewInMemTable())
	go p2.Run(ctx)

	p1.Table().AddPeer(p2.ID(), "localhost:4444")
	if err := p1.Send(ctx, p2.ID(), peer.Message{Content: []byte("Hello")}); err != nil {
		panic(err)
	}
	<-time.After(time.Minute)
}
