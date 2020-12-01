package peer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/renproject/aw/dht"
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
	transport *transport.Transport
	syncer    *Syncer
	gossiper  *Gossiper
}

func New(opts Options, transport *transport.Transport, contentResolver dht.ContentResolver) *Peer {
	return &Peer{
		opts:      opts,
		transport: transport,
		syncer:    NewSyncer(opts.SyncerOptions, opts.Filter, transport),
		gossiper:  NewGossiper(opts.GossiperOptions, opts.Filter, transport, contentResolver),
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

func (p *Peer) Ping(ctx context.Context) error {
	return fmt.Errorf("unimplemented")
}

func (p *Peer) Send(ctx context.Context, to id.Signatory, msg wire.Msg) error {
	return p.transport.Send(ctx, to, msg)
}

func (p *Peer) Sync(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	return p.syncer.Sync(ctx, contentID, hint)
}

func (p *Peer) Gossip(ctx context.Context, contentID []byte, subnet *id.Hash) {
	p.gossiper.Gossip(ctx, contentID, subnet)
}

func (p *Peer) Run(ctx context.Context) {
	p.transport.Receive(ctx, func(from id.Signatory, msg wire.Msg) {
		p.syncer.DidReceiveMessage(from, msg)
		p.gossiper.DidReceiveMessage(from, msg)
	})
	p.transport.Run(ctx)
}

func (p *Peer) Receive(ctx context.Context, f func(id.Signatory, wire.Msg)) {
	p.transport.Receive(ctx, f)
}
