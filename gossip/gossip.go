package gossip

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
	"go.uber.org/zap"
)

var (
	// DefaultSubnet is used to refer to all known signatories.
	DefaultSubnet = id.Hash{}
)

type Gossiper struct {
	opts Options
	self id.Signatory

	dht   dht.DHT
	trans *transport.Transport

	r        *rand.Rand
	jobQueue chan struct {
		wire.Address
		wire.Message
	}

	syncResponders   map[id.Hash][]chan []byte
	syncRespondersMu *sync.Mutex
}

func New(opts Options, self id.Signatory, dht dht.DHT, trans *transport.Transport) *Gossiper {
	g := &Gossiper{
		opts: opts,
		self: self,

		dht:   dht,
		trans: trans,

		r: rand.New(rand.NewSource(time.Now().UnixNano())),
		jobQueue: make(chan struct {
			wire.Address
			wire.Message
		}, opts.Alpha*opts.Alpha),

		syncResponders:   make(map[id.Hash][]chan []byte, opts.MaxCapacity),
		syncRespondersMu: new(sync.Mutex),
	}
	g.trans.ListenForPushes(g)
	g.trans.ListenForPulls(g)
	return g
}

func (g *Gossiper) Run(ctx context.Context) {
	g.opts.Logger.Info("gossiping", zap.Int("alpha", g.opts.Alpha))

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-g.jobQueue:
			func() {
				ctx, cancel := context.WithTimeout(ctx, g.opts.Timeout)
				defer cancel()
				if err := g.trans.Send(ctx, job.Address, job.Message); err != nil {
					g.opts.Logger.Error("sending", zap.String("to", job.Address.String()), zap.Error(err))
				}
			}()
		}
	}
}

// Gossip a message throughout the network. The target can be the signatory in
// the DHT, or it can be a subnet in the DHT. If the target is a subnet, then
// the gossiper will attempt to deliver the message to all peers in the subnet.
// If the target is a signatory, then the gossiper will attempt to deliver the
// message to that specific peer. If the target is neither, the message will be
// dropped.
func (g *Gossiper) Gossip(target, hash id.Hash, dataType uint8) {
	addr, ok := g.dht.Addr(id.Signatory(target))
	if ok {
		marshaledPushV1, err := surge.ToBinary(wire.PushV1{
			Subnet:      id.Hash{},
			ContentHash: hash,
			ContentType: dataType,
		})
		if err != nil {
			g.opts.Logger.Fatal("marshaling push", zap.Error(err))
		}
		g.send(addr, wire.Message{
			Version: wire.V1,
			Type:    wire.Push,
			Data:    marshaledPushV1,
		})
		return
	}

	marshaledPushV1, err := surge.ToBinary(wire.PushV1{
		Subnet:      target,
		ContentHash: hash,
		ContentType: dataType,
	})
	if err != nil {
		g.opts.Logger.Fatal("marshaling push", zap.Error(err))
	}
	g.sendToSubnet(target, wire.Message{
		Version: wire.V1,
		Type:    wire.Push,
		Data:    marshaledPushV1,
	})
}

// Sync a message from members of a particular Subnet.
func (g *Gossiper) Sync(ctx context.Context, subnet, hash id.Hash, dataType uint8) ([]byte, error) {
	pullV1 := wire.PullV1{
		Subnet:      subnet,
		ContentHash: hash,
		ContentType: dataType,
	}
	marshaledPullV1, err := surge.ToBinary(pullV1)
	if err != nil {
		g.opts.Logger.Fatal("marshaling pull", zap.Error(err))
	}
	msg := wire.Message{
		Version: wire.V1,
		Type:    wire.Pull,
		Data:    marshaledPullV1,
	}

	// Before sending the message to the subnet, store a responder channel in
	// the gossiper so we can receive the response.
	g.syncRespondersMu.Lock()

	responder := make(chan []byte, 1)
	if len(g.syncResponders[hash]) == 0 {
		// Only send the message to the subnet if the hash does not exist. This
		// ensures we do not send it multiple times.
		g.sendToSubnet(subnet, msg)
	}
	g.syncResponders[hash] = append(g.syncResponders[hash], responder)

	// Do not defer the unlocking of the mutex, because the next statement could
	// block for a substantial amount of time.
	g.syncRespondersMu.Unlock()

	defer func() {
		g.syncRespondersMu.Lock()
		defer g.syncRespondersMu.Unlock()
		for i, ch := range g.syncResponders[hash] {
			// Equality of channels is ok, according to the Go specification.
			if ch == responder {
				// https://github.com/golang/go/wiki/SliceTricks#delete-without-preserving-order
				end := len(g.syncResponders[hash]) - 1
				g.syncResponders[hash][i] = g.syncResponders[hash][end]
				g.syncResponders[hash] = g.syncResponders[hash][:end]
				break
			}
		}
		if len(g.syncResponders[hash]) == 0 {
			delete(g.syncResponders, hash)
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-responder:
		return response, nil
	}
}

func (g *Gossiper) DidReceivePush(version wire.Version, data []byte, from id.Signatory) (wire.Message, error) {
	if version != wire.V1 {
		return wire.Message{}, fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode request.
	//

	pushV1 := wire.PushV1{}
	if err := surge.FromBinary(&pushV1, data); err != nil {
		return wire.Message{}, fmt.Errorf("unmarshaling push: %v", err)
	}

	//
	// Process response.
	//

	if !g.dht.HasContent(pushV1.ContentHash, pushV1.ContentType) {
		g.dht.InsertContent(pushV1.ContentHash, pushV1.ContentType, []byte{})
		// Beacuse we do not have the content associated with this hash, we try
		// to pull the data from the sender.
		fromAddr, ok := g.dht.Addr(from)
		if ok {
			pullV1 := wire.PullV1{
				Subnet:      pushV1.Subnet,
				ContentHash: pushV1.ContentHash,
				ContentType: pushV1.ContentType,
			}
			marshaledPullV1, err := surge.ToBinary(pullV1)
			if err != nil {
				g.opts.Logger.Fatal("marshaling pull", zap.Error(err))
			}
			msg := wire.Message{
				Version: wire.V1,
				Type:    wire.Pull,
				Data:    marshaledPullV1,
			}
			g.send(fromAddr, msg)
		}
	}
	return wire.Message{Version: wire.V1, Type: wire.PushAck, Data: []byte{}}, nil
}

func (g *Gossiper) DidReceivePushAck(version wire.Version, data []byte, from id.Signatory) error {
	if version != wire.V1 {
		return fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode response.
	//

	pushAckV1 := wire.PushAckV1{}
	if err := surge.FromBinary(&pushAckV1, data); err != nil {
		g.opts.Logger.Fatal("unmarshaling push ack", zap.Error(err))
	}

	//
	// Process response.
	//

	return nil
}

func (g *Gossiper) DidReceivePull(version wire.Version, data []byte, from id.Signatory) (wire.Message, error) {
	if version != wire.V1 {
		return wire.Message{}, fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode request.
	//

	pullV1 := wire.PullV1{}
	if err := surge.FromBinary(&pullV1, data); err != nil {
		return wire.Message{}, fmt.Errorf("unmarshaling pull: %v", err)
	}

	//
	// Acknowledge request.
	//

	content, ok := g.dht.Content(pullV1.ContentHash, pullV1.ContentType)
	if !ok {
		// We do not have the content being requested, so we return empty bytes.
		// It is up to the requester to follow up with others in the network.
		return wire.Message{Version: wire.V1, Type: wire.PullAck, Data: []byte{}}, nil
	}

	pullAckV1 := wire.PullAckV1{
		Subnet:      pullV1.Subnet,
		ContentHash: pullV1.ContentHash,
		ContentType: pullV1.ContentType,
		Content:     content,
	}
	pullAckV1Marshaled, err := surge.ToBinary(pullAckV1)
	if err != nil {
		g.opts.Logger.Fatal("marshaling pull", zap.Error(err))
	}
	return wire.Message{Version: wire.V1, Type: wire.PullAck, Data: pullAckV1Marshaled}, nil
}

func (g *Gossiper) DidReceivePullAck(version wire.Version, data []byte, from id.Signatory) error {
	if version != wire.V1 {
		return fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode response.
	//

	if len(data) == 0 {
		// The gossiper that sent this acknowledgement did not have the content
		// that we tried to pull. This is not an error, but it means there is
		// nothing to do.
		return nil
	}
	pullAckV1 := wire.PullAckV1{}
	if err := surge.FromBinary(&pullAckV1, data); err != nil {
		return fmt.Errorf("unmarshaling pull ack: %v", err)
	}

	//
	// Process response.
	//

	g.syncRespondersMu.Lock()
	defer g.syncRespondersMu.Unlock()

	responders, ok := g.syncResponders[pullAckV1.ContentHash]
	if ok {
		// Write the response to any listeners.
		for _, responder := range responders {
			select {
			case responder <- pullAckV1.Content:
			default:
				// The reader is no longer waiting for the response.
			}
		}
	}

	// Only copy the content into the DHT if we do not have this content at the
	// moment.
	if !g.dht.HasContent(pullAckV1.ContentHash, pullAckV1.ContentType) || g.dht.HasEmptyContent(pullAckV1.ContentHash, pullAckV1.ContentType) {
		g.dht.InsertContent(pullAckV1.ContentHash, pullAckV1.ContentType, pullAckV1.Content)
		g.Gossip(pullAckV1.Subnet, pullAckV1.ContentHash, pullAckV1.ContentType)
	}
	return nil
}

func (g *Gossiper) sendToSubnet(subnet id.Hash, msg wire.Message) {
	// var subnetSignatories []id.Signatory
	subnetSignatories := list.New()
	if subnet == DefaultSubnet {
		// If the default subnet hash is provided, return a random subset of all
		// known signatories.
		addrs := g.dht.Addrs(g.opts.Alpha)
		for _, addr := range addrs {
			sig, err := addr.Signatory()
			if err != nil {
				g.opts.Logger.Error("bad signatory", zap.String("address", addr.String()), zap.Error(err))
				continue
			}
			subnetSignatories.PushBack(sig)
		}
	} else {
		for _, sig := range g.dht.Subnet(subnet) {
			subnetSignatories.PushBack(sig)
		}
	}

	// Loop indefinitely until we have sent min(alpha, n) messages.
	alpha := g.opts.Alpha
	numSignatories := subnetSignatories.Len()
	if alpha > numSignatories {
		alpha = numSignatories
	}
	for {
		for sig := subnetSignatories.Front(); sig != nil; sig = sig.Next() {
			// We express an exponential bias for the signatories that are
			// earlier in the queue (i.e. have pubkey hashes that are similar to
			// our own).
			//
			// The smaller the bias, the more connections we are likely to be
			// maintaining at any one time. However, if the bias is too small,
			// we will not maintain any connections and are more likely to be
			// constantly creating new ones on-demand.
			if g.r.Float64() < g.opts.Bias {
				// Get the associated address, and then remove this signatory
				// from the slice so that we do not gossip to it multiple times.
				addr, ok := g.dht.Addr(sig.Value.(id.Signatory))
				subnetSignatories.Remove(sig)
				alpha--
				if ok {
					g.send(addr, msg)
				}
				break
			}
		}
		if alpha == 0 {
			break
		}
	}
}

func (g *Gossiper) send(addr wire.Address, msg wire.Message) {
	select {
	case g.jobQueue <- struct {
		wire.Address
		wire.Message
	}{addr, msg}:
	default:
		g.opts.Logger.Warn("sending", zap.String("to", addr.String()), zap.Error(errors.New("too much back-pressure")))
	}
}
