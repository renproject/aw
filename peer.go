package aw

import (
	"context"

	"github.com/sirupsen/logrus"
)

type PeerOptions struct {
	Logger logrus.FieldLogger
}

type Peer interface {
	Bootstrap(context.Context) error
	Peer(context.Context, PeerID) (PeerAddress, error)
	Peers(context.Context) (PeerAddresses, error)
	NumPeers(context.Context) (int, error)
}

func New(options PeerOptions, sender MessageSender, receiver MessageReceiver) (Peer, EventReceiver) {
	// FIXME: Implement.

	panic("unimplemented")
}
