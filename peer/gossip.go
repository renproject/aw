package peer

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Gossiper struct {
	opts GossiperOptions

	filter    *channel.SyncFilter
	transport *transport.Transport

	subnetsMu *sync.Mutex
	subnets   map[string]id.Hash

	resolverMu *sync.RWMutex
	resolver   dht.ContentResolver
}

func NewGossiper(opts GossiperOptions, filter *channel.SyncFilter, transport *transport.Transport) *Gossiper {
	return &Gossiper{
		opts: opts,

		filter:    filter,
		transport: transport,

		subnetsMu: new(sync.Mutex),
		subnets:   make(map[string]id.Hash, 1024),

		resolverMu: new(sync.RWMutex),
		resolver:   nil,
	}
}

func (g *Gossiper) Resolve(resolver dht.ContentResolver) {
	g.resolverMu.Lock()
	defer g.resolverMu.Unlock()

	g.resolver = resolver
}

func (g *Gossiper) Gossip(ctx context.Context, contentID []byte, subnet *id.Hash) {
	if subnet == nil {
		subnet = &DefaultSubnet
	}

	recipients := []id.Signatory{}
	if subnet.Equal(&DefaultSubnet) {
		recipients = g.transport.Table().Peers(g.opts.Alpha)
	} else {
		if recipients = g.transport.Table().Subnet(*subnet); len(recipients) > g.opts.Alpha {
			recipients = recipients[:g.opts.Alpha]
		}
	}

	msg := wire.Msg{Version: wire.MsgVersion1, To: *subnet, Type: wire.MsgTypePush, Data: contentID}
	for _, recipient := range recipients {
		go func(sendContext context.Context, to id.Signatory) {
			if err := g.transport.Send(sendContext, to, msg); err != nil {
				g.opts.Logger.Error("pushing gossip", zap.String("peer", to.String()), zap.Error(err))
			}
		}(ctx, recipient)
	}
}

func (g *Gossiper) DidReceiveMessage(from id.Signatory, msg wire.Msg) error {
	switch msg.Type {
	case wire.MsgTypePush:
		g.didReceivePush(from, msg)
	case wire.MsgTypePull:
		g.didReceivePull(from, msg)
	case wire.MsgTypeSync:
		if g.filter.Filter(from, msg) {
			return fmt.Errorf("denied gossip message from %v", from)
		}
		g.didReceiveSync(from, msg)
	}
	return nil
}

func (g *Gossiper) didReceivePush(from id.Signatory, msg wire.Msg) {
	if len(msg.Data) == 0 {
		return
	}

	// Check whether the content is already known. This can cause performance
	// bottle-necks if the content resolver is slow.
	g.resolverMu.RLock()
	if g.resolver == nil {
		g.resolverMu.RUnlock()
		return
	}
	if _, ok := g.resolver.QueryContent(msg.Data); ok {
		g.resolverMu.RUnlock()
		return
	}
	g.resolverMu.RUnlock()

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

	if err := g.transport.Send(ctx, from, wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePull,
		To:      id.Hash(from),
		Data:    msg.Data,
	}); err != nil {
		g.opts.Logger.Error("pull", zap.String("peer", from.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(msg.Data)), zap.Error(err))
		return
	}

	wiggleContext, wiggleCancel := context.WithTimeout(context.Background(), g.opts.WiggleTimeout)
	// Cleanup after the synchronisation timeout has passed. This prevents
	// memory leaking in the filter and in the subnets map. It means that until
	// the timeout passes, we will be accepting synchronisation messages for
	// this content ID.
	go func() {
		<-wiggleContext.Done()
		wiggleCancel()

		g.subnetsMu.Lock()
		delete(g.subnets, string(msg.Data))
		g.subnetsMu.Unlock()

		g.filter.Deny(msg.Data)
	}()
}

func (g *Gossiper) didReceivePull(from id.Signatory, msg wire.Msg) {
	if len(msg.Data) == 0 {
		return
	}

	var content []byte
	var contentOk bool
	func() {
		g.resolverMu.RLock()
		defer g.resolverMu.RUnlock()

		if g.resolver == nil {
			return
		}
		content, contentOk = g.resolver.QueryContent(msg.Data)
	}()
	if !contentOk {
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
}

func (g *Gossiper) didReceiveSync(from id.Signatory, msg wire.Msg) {
	g.resolverMu.RLock()
	if g.resolver == nil {
		g.resolverMu.RUnlock()
		return
	}

	_, alreadySeenContent := g.resolver.QueryContent(msg.Data)
	if alreadySeenContent {
		g.resolverMu.RUnlock()
		return
	}
	if len(msg.Data) == 0 || len(msg.SyncData) == 0 {
		g.resolverMu.RUnlock()
		return
	}

	// We are relying on the correctness of the channel filtering to ensure that
	// no synchronisation messages reach the gossiper unless the gossiper (or
	// the synchroniser) have allowed them.
	g.resolver.InsertContent(msg.Data, msg.SyncData)
	g.resolverMu.RUnlock()

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
	g.Gossip(ctx, msg.Data, &subnet)
}
