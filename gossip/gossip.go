package gossip

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
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
	g.opts.Logger.Infof("gossiping with alpha=%v", g.opts.Alpha)

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-g.jobQueue:
			func() {
				ctx, cancel := context.WithTimeout(ctx, g.opts.Timeout)
				defer cancel()
				if err := g.trans.Send(ctx, job.Address, job.Message); err != nil {
					g.opts.Logger.Errorf("sending to address=%v: %v", job.Address, err)
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
			Subnet: id.Hash{},
			Hash:   hash,
			Type:   dataType,
		})
		if err != nil {
			g.opts.Logger.Fatalf("marshaling push: %v", err)
		}
		g.send(addr, wire.Message{
			Version: wire.V1,
			Type:    wire.Push,
			Data:    marshaledPushV1,
		})
		return
	}

	marshaledPushV1, err := surge.ToBinary(wire.PushV1{
		Subnet: target,
		Hash:   hash,
		Type:   dataType,
	})
	if err != nil {
		g.opts.Logger.Fatalf("marshaling push: %v", err)
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
		Subnet: subnet,
		Hash:   hash,
		Type:   dataType,
	}
	marshaledPullV1, err := surge.ToBinary(pullV1)
	if err != nil {
		g.opts.Logger.Fatalf("marshaling pull: %v", err)
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

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-responder:
		return response, nil
	}
}

func (g *Gossiper) DidReceivePush(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if version != wire.V1 {
		return wire.Message{}, fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode request.
	//

	pushV1 := wire.PushV1{}
	if err := surge.FromBinary(data, &pushV1); err != nil {
		return wire.Message{}, fmt.Errorf("unmarshaling push: %v", err)
	}

	//
	// Process response.
	//

	if !g.dht.HasContent(pushV1.Hash, pushV1.Type) {
		g.dht.InsertContent(pushV1.Hash, pushV1.Type, []byte{})
		// Beacuse we do not have the content associated with this hash, we try
		// to pull the data from the sender.
		fromAddr, ok := g.dht.Addr(from)
		if ok {
			pullV1 := wire.PullV1{
				Subnet: pushV1.Subnet,
				Hash:   pushV1.Hash,
				Type:   pushV1.Type,
			}
			marshaledPullV1, err := surge.ToBinary(pullV1)
			if err != nil {
				g.opts.Logger.Fatalf("marshaling pull: %v", err)
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

func (g *Gossiper) DidReceivePushAck(version uint8, data []byte, from id.Signatory) error {
	if version != wire.V1 {
		return fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode response.
	//

	pushAckV1 := wire.PushAckV1{}
	if err := surge.FromBinary(data, &pushAckV1); err != nil {
		g.opts.Logger.Fatalf("unmarshaling push ack: %v", err)
	}

	//
	// Process response.
	//

	return nil
}

func (g *Gossiper) DidReceivePull(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if version != wire.V1 {
		return wire.Message{}, fmt.Errorf("unsupported version=%v", version)
	}

	//
	// Decode request.
	//

	pullV1 := wire.PullV1{}
	if err := surge.FromBinary(data, &pullV1); err != nil {
		return wire.Message{}, fmt.Errorf("unmarshaling pull: %v", err)
	}

	//
	// Acknowledge request.
	//

	content, ok := g.dht.Content(pullV1.Hash, pullV1.Type)
	if !ok {
		// We do not have the content being requested, so we return empty bytes.
		// It is up to the requester to follow up with others in the network.
		return wire.Message{Version: wire.V1, Type: wire.PullAck, Data: []byte{}}, nil
	}

	pullAckV1 := wire.PullAckV1{
		Subnet:  pullV1.Subnet,
		Hash:    pullV1.Hash,
		Type:    pullV1.Type,
		Content: content,
	}
	pullAckV1Marshaled, err := surge.ToBinary(pullAckV1)
	if err != nil {
		g.opts.Logger.Fatalf("marshaling pull: %v", err)
	}
	return wire.Message{Version: wire.V1, Type: wire.PullAck, Data: pullAckV1Marshaled}, nil
}

func (g *Gossiper) DidReceivePullAck(version uint8, data []byte, from id.Signatory) error {
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
	if err := surge.FromBinary(data, &pullAckV1); err != nil {
		return fmt.Errorf("unmarshaling pull ack: %v", err)
	}

	//
	// Process response.
	//

	g.syncRespondersMu.Lock()
	defer g.syncRespondersMu.Unlock()

	responders, ok := g.syncResponders[pullAckV1.Hash]
	if ok {
		// Write the response to any listeners.
		for _, responder := range responders {
			select {
			case responder <- pullAckV1.Content:
			default:
				// The reader is no longer waiting for the response.
			}
		}

		// Clean up the map.
		//
		// TODO: There needs to be a way to prune this map if the gossiper
		// does not receive a pull acknowledgement (or receives one but the
		// content is empty so it does not get to this stage).
		delete(g.syncResponders, pullAckV1.Hash)
	}

	// Only copy the content into the DHT if we do not have this content at the
	// moment.
	if !g.dht.HasContent(pullAckV1.Hash, pullAckV1.Type) || g.dht.HasEmptyContent(pullAckV1.Hash, pullAckV1.Type) {
		g.dht.InsertContent(pullAckV1.Hash, pullAckV1.Type, pullAckV1.Content)
		g.Gossip(pullAckV1.Subnet, pullAckV1.Hash, pullAckV1.Type)
	}
	return nil
}

func (g *Gossiper) sendToSubnet(subnet id.Hash, msg wire.Message) {
	var subnetSignatories []id.Signatory
	if subnet == DefaultSubnet {
		// If the default subnet hash is provided, return a random subset of all
		// known signatories.
		addrs := g.dht.Addrs(g.opts.Alpha)
		subnetSignatories = make([]id.Signatory, 0, len(addrs))
		for _, addr := range addrs {
			sig, err := addr.Signatory()
			if err != nil {
				g.opts.Logger.Errorf("failed to get signatory from %v: err", addr.String(), err)
				continue
			}
			subnetSignatories = append(subnetSignatories, sig)
		}
	} else {
		subnetSignatories = g.dht.Subnet(subnet) // TODO: Load signatories in order of their XOR distance from our own address.
	}

	for a := 0; a < g.opts.Alpha; a++ {
		for i := 0; i < len(subnetSignatories); i++ {
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
				addr, ok := g.dht.Addr(subnetSignatories[i])
				subnetSignatories = append(subnetSignatories[:i], subnetSignatories[i+1:]...)
				i--
				if ok {
					g.send(addr, msg)
				}
				break
			}
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
		g.opts.Logger.Warnf("sending to address=%v: too much back-pressure", addr)
	}
}
