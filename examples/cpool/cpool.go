package main

import (
	"context"
	"math/rand"
	"net"
	"time"

	"github.com/renproject/id"

	"github.com/renproject/aw/experiment/channel"
	"github.com/renproject/aw/experiment/codec"

	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/aw/experiment/policy"

	"github.com/renproject/aw/experiment/tcp"
)

func main() {
	serverPrivKey := id.NewPrivKey()
	clientPrivKey := id.NewPrivKey()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cpool := channel.NewPool()

	go tcp.Listen(
		ctx,
		"localhost:8080",
		func(c net.Conn) {
			h := handshake.ECIESServerHandshake(
				serverPrivKey,
				rand.New(rand.NewSource(time.Now().UnixNano())))
			h = cpool.HighestPeerWinsHandshake(id.NewSignatory(serverPrivKey.PubKey()), h)
			_, _, _, err := h(
				c,
				codec.LengthPrefixEncoder(codec.PlainEncoder),
				codec.LengthPrefixDecoder(codec.PlainDecoder))
			if err != nil {
				cancel()
				return
			}
		}, func(err error) {

		},
		policy.Max(3))

	tcp.Dial(
		ctx,
		"localhost:8080",
		func(c net.Conn) {
			h := handshake.ECIESClientHandshake(
				clientPrivKey,
				serverPrivKey.PubKey(),
				rand.New(rand.NewSource(time.Now().UnixNano())))
			h = cpool.HighestPeerWinsHandshake(id.NewSignatory(clientPrivKey.PubKey()), h)
			_, _, _, err := h(
				c,
				codec.LengthPrefixEncoder(codec.PlainEncoder),
				codec.LengthPrefixDecoder(codec.PlainDecoder))
			if err != nil {
				cancel()
				return
			}
		}, func(err error) {

		},
		policy.ConstantTimeout(30*time.Second))
}
