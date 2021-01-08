package peer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/renproject/aw/channel"
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
	discoveryClient *DiscoveryClient
}

func New(opts Options, transport *transport.Transport) *Peer {
	filter := channel.NewSyncFilter()
	return &Peer{
		opts:      opts,
		transport: transport,
		syncer:    NewSyncer(opts.SyncerOptions, filter, transport),
		gossiper:  NewGossiper(opts.GossiperOptions, filter, transport),
		discoveryClient: NewDiscoveryClient(opts.DiscoveryOptions, transport, contentResolver),
	}
}

func (p *Peer) ID() id.Signatory {
	return p.opts.PrivKey.Signatory()
}

func (p *Peer) Syncer() *Syncer {
	return p.syncer
}

func (p *Peer) Gossiper() *Gossiper {
	return p.gossiper
}

func (p *Peer) Transport() *transport.Transport {
	return p.transport
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

func (p *Peer) PeerDiscovery(ctx context.Context) {
	p.discoveryClient.DiscoverPeers(ctx)
}

func (p *Peer) Run(ctx context.Context) {
	p.transport.Receive(ctx, func(from id.Signatory, msg wire.Msg) error {
		if msg.Type == wire.MsgTypePing || msg.Type == wire.MsgTypePingAck {
			p.discoveryClient.DidReceiveMessage(from, msg)
			return nil
		}
		if err := p.syncer.DidReceiveMessage(from, msg); err != nil {
			return err
		}
		if err := p.gossiper.DidReceiveMessage(from, msg); err != nil {
			return err
		}
		return nil
	})
	p.transport.Run(ctx)
}

func (p *Peer) Receive(ctx context.Context, f func(id.Signatory, wire.Msg) error) {
	p.transport.Receive(ctx, f)
}

func (p *Peer) Resolve(ctx context.Context, contentResolver dht.ContentResolver) {
	p.gossiper.Resolve(contentResolver)
}
