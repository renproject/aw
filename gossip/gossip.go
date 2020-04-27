package gossip

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type Receiver interface {
	DidReceiveContent(hash id.Hash, content []byte)
}

type Gossiper struct {
	opts Options

	receiver Receiver
	dht      dht.DHT
	trans    *transport.Transport

	r *rand.Rand
}

func New(opts Options, receiver Receiver, dht dht.DHT, trans *transport.Transport) *Gossiper {
	return &Gossiper{
		opts: opts,

		receiver: receiver,
		dht:      dht,
		trans:    trans,

		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Gossip a message to members of a particular Subnet.
func (g *Gossiper) Gossip(ctx context.Context, subnet, contentHash id.Hash) {
	pushV1 := wire.PushV1{
		Subnet: subnet,
		Hash:   contentHash,
	}
	marshaledPushV1, err := surge.ToBinary(pushV1)
	if err != nil {
		g.opts.Logger.Fatalf("marshaling push: %v", err)
	}
	msg := wire.Message{
		Version: wire.V1,
		Type:    wire.Push,
		Data:    marshaledPushV1,
	}

	subnetSignatories := g.dht.Subnet(subnet)
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
					if err := g.trans.Send(ctx, addr, msg); err != nil {
						g.opts.Logger.Warnf("pushing to address=%v: %v", addr, err)
						g.opts.Logger.Infof("deleting address=%v", addr)
						g.dht.DeleteAddr(subnetSignatories[i])
					}
				}
				break
			}
		}
	}
}

// Sync a message from members of a particular Subnet.
func (g *Gossiper) Sync(ctx context.Context, subnet, contentHash id.Hash) {
	pullV1 := wire.PullV1{
		Subnet: subnet,
		Hash:   contentHash,
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

	subnetSignatories := g.dht.Subnet(subnet)
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
					if err := g.trans.Send(ctx, addr, msg); err != nil {
						g.opts.Logger.Warnf("pushing to address=%v: %v", addr, err)
						g.opts.Logger.Infof("deleting address=%v", addr)
						g.dht.DeleteAddr(subnetSignatories[i])
					}
				}
				break
			}
		}
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

	if !g.dht.HasContent(pushV1.Hash) {
		g.dht.InsertContent(pushV1.Hash, []byte{})

		// Beacuse we do not have the content associated with this hash, we try
		// to pull the data from the sender.
		fromAddr, ok := g.dht.Addr(from)
		if ok {
			pullV1 := wire.PullV1{
				Subnet: pushV1.Subnet,
				Hash:   pushV1.Hash,
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
			g.trans.Send(context.TODO(), fromAddr, msg)
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

	content, ok := g.dht.Content(pullV1.Hash)
	if !ok {
		// We do not have the content being requested, so we return empty bytes.
		// It is up to the requester to follow up with others in the network.
		return wire.Message{Version: wire.V1, Type: wire.PullAck, Data: []byte{}}, nil
	}

	pullAckV1 := wire.PullAckV1{
		Subnet:  pullV1.Subnet,
		Hash:    pullV1.Hash,
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

	// Only copy the content into the DHT if we do not have this content at the
	// moment.
	if !g.dht.HasContent(pullAckV1.Hash) || g.dht.HasEmptyContent(pullAckV1.Hash) {
		g.dht.InsertContent(pullAckV1.Hash, pullAckV1.Content)
		g.receiver.DidReceiveContent(pullAckV1.Hash, pullAckV1.Content)

		go func() {
			// Wait 1 millisecond before continuing the gossip (to increase the
			// probability that we will successfully synchronise the data being
			// gossiped).
			g.Gossip(context.TODO(), pullAckV1.Subnet, pullAckV1.Hash)
		}()
	}
	return nil
}
