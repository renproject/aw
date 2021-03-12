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

// ErrPeerNotFound represents the error raised when a designated recipient of a message
// is not present in the peer table
var (
	ErrPeerNotFound = errors.New("peer not found")
)

type Peer struct {
	opts            Options
	transport       *transport.Transport
	syncer          *Syncer
	gossiper        *Gossiper
	discoveryClient *DiscoveryClient
}

func New(opts Options, transport *transport.Transport) *Peer {
	filter := channel.NewSyncFilter()
	return &Peer{
		opts:            opts,
		transport:       transport,
		syncer:          NewSyncer(opts.SyncerOptions, filter, transport),
		gossiper:        NewGossiper(opts.GossiperOptions, filter, transport),
		discoveryClient: NewDiscoveryClient(opts.DiscoveryOptions, transport),
	}
}

// ID return the signatory value associated with the peer
func (p *Peer) ID() id.Signatory {
	return p.opts.PrivKey.Signatory()
}

// Syncer method return a pointer to the syncer struct associated with the peer
func (p *Peer) Syncer() *Syncer {
	return p.syncer
}

// Gossiper method return a pointer to the gossiper struct associated with the peer
func (p *Peer) Gossiper() *Gossiper {
	return p.gossiper
}

// Transport returns a pointer to the transport layer associated with the peer
func (p *Peer) Transport() *transport.Transport {
	return p.transport
}

// Link accepts a signatory registers a peer as a trusted individual for
// persistent connections
func (p *Peer) Link(remote id.Signatory) {
	p.transport.Link(remote)
}

// Link accepts a signatory and de-registers a peer as a trusted individual
// for persistent connections
func (p *Peer) Unlink(remote id.Signatory) {
	p.transport.Unlink(remote)
}

// Ping accepts a context and a signatory and is used to send a ping message to a peer.
// It can be used to check liveness of a peer
func (p *Peer) Ping(ctx context.Context, to id.Signatory) error {
	return fmt.Errorf("unimplemented")
}

// Send accept a context, the signatory ID of a recipient peer, and a message. It tries to send
// the message to the recipient peer using the transport layer until the context expires
func (p *Peer) Send(ctx context.Context, to id.Signatory, msg wire.Msg) error {
	return p.transport.Send(ctx, to, msg)
}

// Sync accepts a context, an array of bytes (contentID) and a pointer to a signatory (hint).
// A call to Sync is used to request content with a particular contentID from peers.
// A hint can be supplied to try syncing from a particular peer as well.
// A nil hint will indicate that the content can be attempted to be synced any alpha random peers,
// where alpha is defined in SyncerOptions struct
func (p *Peer) Sync(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	return p.syncer.Sync(ctx, contentID, hint)
}

// Gossip accepts a context, an array of bytes (contentID) and a pointer to a id.Hash (subnet)
// A call to Gossip is used to gossip information (content) with a particular contentID with peers.
// A subnet can be supplied to try gossip with a particular subnet of peers.
// If the supplied subnet id DefaultSubnet or nil, content will be gossiped with alpha random peers,
// where alpha is defined in GossiperOptions struct.
func (p *Peer) Gossip(ctx context.Context, contentID []byte, subnet *id.Hash) {
	p.gossiper.Gossip(ctx, contentID, subnet)
}

// DiscoverPeers accepts a context and is used to start discovering new peers using the
// peer discovery client. DiscoverPeers is a blocking method, ideally it would be called
// as `go p.DiscoverPeers(...)`
func (p *Peer) DiscoverPeers(ctx context.Context) {
	p.discoveryClient.DiscoverPeers(ctx)
}

// Run accepts a context and sets up the default receive function the features callbacks
// for syncer, gossiper and peer discovery. Finally, it calls the Run method on the transport
// layer, which then starts to listen for and accept connections.
// A call to Run is a blocking method, ideally it would be called as `go p.Run(...)`
func (p *Peer) Run(ctx context.Context) {
	p.transport.Receive(ctx, func(from id.Signatory, msg wire.Msg) error {
		if err := p.syncer.DidReceiveMessage(from, msg); err != nil {
			return err
		}
		if err := p.gossiper.DidReceiveMessage(from, msg); err != nil {
			return err
		}
		if err := p.discoveryClient.DidReceiveMessage(from, msg); err != nil {
			return err
		}
		return nil
	})
	p.transport.Run(ctx)
}

// Receive takes a context and a custom receive handler. It is used to execute custom
// logic on messages. Every message, along with the ID (signatory) of the sender is
// passed to this function in a sequential manner.
func (p *Peer) Receive(ctx context.Context, f func(id.Signatory, wire.Msg) error) {
	p.transport.Receive(ctx, f)
}

// Resolve takes in a context and a ContentResolver and associates it with
// the gossiper. Gossiper / Sync logic will not function until a valid
// ContentResolver is provided to a call to Resolve
func (p *Peer) Resolve(ctx context.Context, contentResolver dht.ContentResolver) {
	p.gossiper.Resolve(contentResolver)
}
