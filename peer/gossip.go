package peer

import (
	"context"
	"encoding/base64"
	"sync"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type GossiperOptions struct {
	Logger  *zap.Logger
	Alpha   int
	Timeout time.Duration
}

func DefaultGossiperOptions() GossiperOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return GossiperOptions{
		Logger:  logger,
		Alpha:   DefaultAlpha,
		Timeout: DefaultTimeout,
	}
}

type Gossiper struct {
	opts GossiperOptions

	filter    *channel.SyncFilter
	transport *transport.Transport
	resolver  dht.ContentResolver
	next      Receiver

	subnetsMu *sync.Mutex
	subnets   map[string]id.Hash
}

func NewGossiper(opts GossiperOptions, filter *channel.SyncFilter, transport *transport.Transport, resolver dht.ContentResolver, next Receiver) *Gossiper {
	return &Gossiper{
		opts: opts,

		filter:    filter,
		transport: transport,
		resolver:  resolver,
		next:      next,

		subnetsMu: new(sync.Mutex),
		subnets:   make(map[string]id.Hash, 1024),
	}
}

func (g *Gossiper) Gossip(ctx context.Context, subnet id.Hash, contentID []byte) {
	recipients := []id.Signatory{}
	if subnet.Equal(&DefaultSubnet) {
		recipients = g.transport.Table().Addresses(g.opts.Alpha)
	} else {
		recipients = g.transport.Table().Subnet(subnet)
	}

	msg := wire.Msg{Version: wire.MsgVersion1, To: subnet, Type: wire.MsgTypePush, Data: contentID}
	for _, recipient := range recipients {
		if err := g.transport.Send(ctx, recipient, msg); err != nil {
			g.opts.Logger.Error("pushing gossip", zap.String("peer", recipient.String()), zap.Error(err))
		}
	}
}

func (g *Gossiper) DidReceiveMessage(from id.Signatory, msg wire.Msg) {
	switch msg.Type {
	case wire.MsgTypePush:
		g.didReceivePush(from, msg)
	case wire.MsgTypePull:
		g.didReceivePull(from, msg)
	case wire.MsgTypeSync:
		g.didReceiveSync(from, msg)
	}
	if g.next != nil {
		g.next.DidReceiveMessage(from, msg)
	}
}

func (g *Gossiper) didReceivePush(from id.Signatory, msg wire.Msg) {
	if len(msg.Data) == 0 {
		return
	}
	if _, ok := g.resolver.Content(msg.Data); ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.opts.Timeout)
	defer cancel()

	// Later, we will probably receive a synchronisation message for the content
	// associated with this push. We store the subnet now, so that we know how
	// to propagate the content later.
	g.subnetsMu.Lock()
	g.subnets[string(msg.Data)] = msg.To
	g.subnetsMu.Unlock()

	// We are expecting a synchronisation message, because we are about to send
	// out a pull message. So, we need to allow the content in the filter.
	g.filter.Allow(msg.Data)

	// Cleanup after the synchronisation timeout has passed. This prevents
	// memory leaking in the filter and in the subnets map. It means that until
	// the timeout passes, we will be accepting synchronisation messages for
	// this content ID.
	go func() {
		<-ctx.Done()

		g.subnetsMu.Lock()
		delete(g.subnets, string(msg.Data))
		g.subnetsMu.Unlock()

		g.filter.Deny(msg.Data)
	}()

	if err := g.transport.Send(ctx, from, wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePull,
		To:      id.Hash(from),
		Data:    msg.Data,
	}); err != nil {
		g.resolver.Delete(msg.Data)
		g.opts.Logger.Error("pull", zap.String("peer", from.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(msg.Data)), zap.Error(err))
		return
	}
}

func (g *Gossiper) didReceivePull(from id.Signatory, msg wire.Msg) {
	if len(msg.Data) == 0 {
		return
	}

	content, ok := g.resolver.Content(msg.Data)
	if !ok {
		g.opts.Logger.Debug("content not found", zap.String("peer", from.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(msg.Data)))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.opts.Timeout)
	defer cancel()

	if err := g.transport.Send(ctx, from, wire.Msg{
		Version:  wire.MsgVersion1,
		To:       id.Hash(from),
		Type:     wire.MsgTypeSync,
		Data:     msg.Data,
		SyncData: content,
	}); err != nil {
		g.opts.Logger.Error("sync", zap.String("peer", from.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(msg.Data)), zap.Error(err))
	}
	return
}

func (g *Gossiper) didReceiveSync(from id.Signatory, msg wire.Msg) {
	_, alreadySeenContent := g.resolver.Content(msg.Data)
	if alreadySeenContent {
		return
	}
	if len(msg.Data) == 0 || len(msg.SyncData) == 0 {
		return
	}

	// We are relying on the correctness of the channel filtering to ensure that
	// no synchronisation messages reach the gossiper unless the gossiper (or
	// the synchroniser) have allowed them.
	g.resolver.Insert(msg.Data, msg.SyncData)

	g.subnetsMu.Lock()
	subnet, ok := g.subnets[string(msg.Data)]
	g.subnetsMu.Unlock()

	if !ok {
		// The gossip has taken too long, and the subnet was removed from the
		// map to preseve memory. Gossiping cannot continue.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.opts.Timeout)
	defer cancel()

	g.Gossip(ctx, subnet, msg.Data)
}
