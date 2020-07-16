package aw

import (
	"context"
	"crypto/sha256"
	"sync"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/gossip"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Builder struct {
	opts Options

	handshaker handshake.Options
	trans      transport.Options
	peer       peer.Options
	gossiper   gossip.Options

	dht             dht.DHT
	contentResolver dht.ContentResolver
}

func New() *Builder {
	builder := &Builder{
		opts: DefaultOptions(),

		handshaker: handshake.DefaultOptions(),
		trans:      transport.DefaultOptions(),
		peer:       peer.DefaultOptions(),
		gossiper:   gossip.DefaultOptions(),

		contentResolver: dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil),
	}
	// By default, the content resolver is nil, meaning content will only be
	// stored in-memory.
	builder.dht = dht.New(
		id.NewSignatory((*id.PubKey)(&builder.handshaker.PrivKey.PublicKey)),
		builder.contentResolver,
	)
	return builder
}

func (builder *Builder) WithPrivKey(privKey *id.PrivKey) *Builder {
	builder.handshaker.PrivKey = privKey
	builder.dht = dht.New(
		id.NewSignatory((*id.PubKey)(&builder.handshaker.PrivKey.PublicKey)),
		builder.contentResolver,
	)
	if err := builder.peer.Addr.Sign(builder.handshaker.PrivKey); err != nil {
		builder.opts.Logger.Fatal("signing address", zap.String("address", builder.peer.Addr.String()), zap.Error(err))
	}
	return builder
}

func (builder *Builder) WithContentResolver(contentResolver dht.ContentResolver) *Builder {
	builder.contentResolver = contentResolver
	builder.dht = dht.New(
		id.NewSignatory((*id.PubKey)(&builder.handshaker.PrivKey.PublicKey)),
		builder.contentResolver,
	)
	return builder
}

func (builder *Builder) WithAddr(addr wire.Address) *Builder {
	builder.peer.Addr = addr
	if err := builder.peer.Addr.Sign(builder.handshaker.PrivKey); err != nil {
		builder.opts.Logger.Fatal("signing address", zap.String("address", addr.String()), zap.Error(err))
	}
	return builder
}
func (builder *Builder) WithHost(host string) *Builder {
	builder.trans.TCPServerOpts = builder.trans.TCPServerOpts.WithHost(host)
	return builder
}
func (builder *Builder) WithPort(port uint16) *Builder {
	builder.trans.TCPServerOpts = builder.trans.TCPServerOpts.WithPort(port)
	return builder
}

func (builder *Builder) Build() *Node {
	handshaker := handshake.NewECDSA(builder.handshaker)
	trans := transport.New(builder.trans, handshaker)
	peer := peer.New(builder.peer, builder.dht, trans, builder.handshaker.PrivKey)
	gossiper := gossip.New(builder.gossiper, peer.Identity(), builder.dht, trans)
	return &Node{
		opts:     builder.opts,
		dht:      builder.dht,
		trans:    trans,
		peer:     peer,
		gossiper: gossiper,
	}
}

type Node struct {
	opts Options

	dht      dht.DHT
	trans    *transport.Transport
	peer     *peer.Peer
	gossiper *gossip.Gossiper
}

func (node *Node) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		node.trans.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		node.peer.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		node.gossiper.Run(ctx)
	}()

	wg.Wait()
}

func (node *Node) Send(ctx context.Context, signatory id.Signatory, dataType uint8, data []byte) {
	hash := sha256.Sum256(data)
	node.dht.InsertContent(hash, dataType, data)
	node.gossiper.Gossip(id.Hash(signatory), hash, dataType)
}

func (node *Node) Broadcast(ctx context.Context, subnet id.Hash, dataType uint8, data []byte) {
	hash := sha256.Sum256(data)
	node.dht.InsertContent(hash, dataType, data)
	node.gossiper.Gossip(subnet, hash, dataType)
}

func (node *Node) DHT() dht.DHT {
	return node.dht
}

func (node *Node) Transport() *transport.Transport {
	return node.trans
}

func (node *Node) Peer() *peer.Peer {
	return node.peer
}

func (node *Node) Gossiper() *gossip.Gossiper {
	return node.gossiper
}

func (node *Node) Identity() id.Signatory {
	return node.peer.Identity()
}

func (node *Node) Addr() wire.Address {
	return node.peer.Addr()
}
