package peer

import (
	"context"
	"errors"
	"fmt"

	"github.com/renproject/aw/channel"
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
	opts      Options
	table     Table
	transport *transport.Transport
}

func New(opts Options, table Table, transport *transport.Transport) *Peer {
	return &Peer{
		opts:      opts,
		table:     table,
		transport: transport,
	}
}

func (p *Peer) ID() id.Signatory {
	return p.opts.PrivKey.Signatory()
}

func (p *Peer) Table() Table {
	return p.table
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
	toAddr, ok := p.table.PeerAddress(to)
	if !ok {
		return fmt.Errorf("%v not found", to)
	}
	return p.transport.Send(ctx, to, toAddr, msg)
}

func (p *Peer) Gossip(ctx context.Context, subnet id.Hash, mgs wire.Msg) error {
	panic("unimplemented")
}

// Run the peer until the context is done. If running encounters an error, or
// panics, it will automatically recover and continue until the context is done.
func (p *Peer) Run(ctx context.Context) {
	receiver := make(chan channel.Msg)
	go func() {
		select {
		case <-ctx.Done():
		case msg := <-receiver:
			p.opts.Callbacks.DidReceiveMessage(msg.From, msg.Msg)
		}
	}()
	go p.transport.Receive(ctx, receiver)
	p.transport.Run(ctx)
}
