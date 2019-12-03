package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/sirupsen/logrus"
)

func NewFullyConnectedPeers(b, n int) ([]peer.Peer, []chan protocol.Event) {
	signVerifiers := NewSignVerifiers(n)
	addrs := make([]protocol.PeerAddress, n)
	for i := range addrs {
		addrs[i] = NewSimpleTCPPeerAddress(signVerifiers[i].ID(), "0.0.0.0", fmt.Sprintf("%v", 8000+i))
	}

	options := peer.Options{
		Me:                   nil,
		BootstrapAddresses:   addrs[:b],
		DisablePeerDiscovery: false,
		Capacity:             128,
	}

	peers := make([]peer.Peer, n)
	events := make([]chan protocol.Event, n)
	for i := range peers {
		logger := logrus.New().WithField("node", i)
		options.Me = addrs[i]
		events[i] = make(chan protocol.Event, 1024)
		poolOption := tcp.ConnPoolOptions{}
		serverOption := tcp.ServerOptions{
			Host:      fmt.Sprintf(":%v", 8000+i),
			RateLimit: time.Duration(-1), // disable this due to all peers have the same ip address in local test.
		}

		peers[i] = peer.NewTCP(logger.WithField("node", i), SimpleTCPPeerAddressCodec{}, options, events[i], signVerifiers[i], poolOption, serverOption)
	}
	return peers, events
}

func NewSignVerifiers(n int) []MockSignVerifier {
	SignVerifiers := make([]MockSignVerifier, n)
	for i := range SignVerifiers {
		SignVerifiers[i] = NewMockSignVerifier()
	}
	for i := range SignVerifiers {
		for j := range SignVerifiers {
			SignVerifiers[i].Whitelist(SignVerifiers[j].ID())
		}
	}
	return SignVerifiers
}

func RandomSenderAndReceiver(n int) (int, int) {
	sender := rand.Intn(n)
	receiver := rand.Intn(n)
	for receiver == sender {
		receiver = rand.Intn(n)
	}

	return sender, receiver
}

func ReadChannel(ctx context.Context, events chan protocol.Event) (protocol.EventMessageReceived, bool) {
	for {
		select {
		case event := <-events:
			switch e := event.(type) {
			case protocol.EventPeerChanged:
				continue
			case protocol.EventMessageReceived:
				return e, true
			default:
				panic("unknown event type")
			}
		case <-ctx.Done():
			return protocol.EventMessageReceived{}, false
		}
	}
}

func EmptyChannel(events chan protocol.Event) {
	for {
		select {
		case <-events:
			continue
		default:
			return
		}
	}
}
