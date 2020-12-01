package peer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

var (
	DefaultSubnet  = id.Hash{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	DefaultAlpha   = 5
	DefaultTimeout = time.Second
)

var (
	ErrPeerNotFound = errors.New("peer not found")
)

type Peer struct {
	opts      Options
	gossiper  *Gossiper
	syncer    *Syncer
	transport *transport.Transport
}

func New(opts Options, gossiper *Gossiper, syncer *Syncer, transport *transport.Transport) *Peer {
	return &Peer{
		opts:      opts,
		gossiper:  gossiper,
		syncer:    syncer,
		transport: transport,
	}
}

func (p *Peer) ID() id.Signatory {
	return p.opts.PrivKey.Signatory()
}

func (p *Peer) Link(remote id.Signatory) {
	p.transport.Link(remote)
}

func (p *Peer) Unlink(remote id.Signatory) {
	p.transport.Unlink(remote)
}

// Ping initiates a round of peer discovery in the network. The peer will
// attempt to gossip its identity throughout the network, and discover the
// identity of other remote peers in the network. It will continue doing so
// until the context is done.
func (p *Peer) Ping(ctx context.Context) error {
	return fmt.Errorf("unimplemented")
}

func (p *Peer) Send(ctx context.Context, to id.Signatory, msg wire.Msg) error {
	return p.transport.Send(ctx, to, msg)
}

func (p *Peer) Gossip(ctx context.Context, contentID, content []byte) error {
	return fmt.Errorf("unimplemented")
}

func (p *Peer) Sync(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	return p.syncer.Sync(ctx, contentID, hint)
}

// Run the peer until the context is done. If running encounters an error, or
// panics, it will automatically recover and continue until the context is done.
func (p *Peer) Run(ctx context.Context) {
	p.transport.Receive(ctx, func(from id.Signatory, msg wire.Msg) {
		p.opts.Receiver.DidReceiveMessage(from, msg)
		p.gossiper.DidReceiveMessage(from, msg)
		p.syncer.DidReceiveMessage(from, msg)
	})
	p.transport.Run(ctx)
}
