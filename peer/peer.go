package peer

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

var (
	// GlobalSubnet is a reserved subnet identifier that is used to reference
	// the entire peer-to-peer network.
	GlobalSubnet = id.Hash{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

var (
	ErrPeerNotFound = errors.New("peer not found")
)

type Peer struct {
	opts            Options
	syncer          *Syncer
	transport       *transport.Transport
	contentResolver dht.ContentResolver
}

func New(opts Options, syncer *Syncer, transport *transport.Transport, contentResolver dht.ContentResolver) *Peer {
	return &Peer{
		opts:            opts,
		syncer:          syncer,
		transport:       transport,
		contentResolver: contentResolver,
	}
}

func (p *Peer) ID() id.Signatory {
	return p.opts.PrivKey.Signatory()
}

func (p *Peer) Table() dht.Table {
	return p.transport.Table()
}

func (p *Peer) ContentResolver() dht.ContentResolver {
	return p.contentResolver
}

func (p *Peer) Logger() *zap.Logger {
	return p.opts.Logger
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
	panic("unimplemented")
}

func (p *Peer) Send(ctx context.Context, to id.Signatory, msg wire.Msg) error {
	return p.transport.Send(ctx, to, msg)
}

func (p *Peer) Gossip(ctx context.Context, contentID, content []byte) error {
	return fmt.Errorf("unimplemented")
}

func (p *Peer) Sync(ctx context.Context, contentID []byte) ([]byte, error) {
	return p.syncer.Sync(ctx, contentID, nil)
}

// Run the peer until the context is done. If running encounters an error, or
// panics, it will automatically recover and continue until the context is done.
func (p *Peer) Run(ctx context.Context) {
	p.transport.Receive(ctx, func(from id.Signatory, msg wire.Msg) {
		p.opts.Receiver.DidReceiveMessage(from, msg)
	})
	p.transport.Run(ctx)
}
