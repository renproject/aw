package peer

import (
	"context"
	"fmt"
	
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Callbacks struct {
	DidReceiveMessage func(p *Peer, from id.Signatory, msg wire.Msg)
}

func DefaultDidReceiveMessage(p *Peer, remote id.Signatory, msg wire.Msg) {
	switch msg.Type {
	case wire.MsgTypeSync:
		DefaultDidReceiveSync(p, remote, msg)
	case wire.MsgTypeSyncAck:
		DefaultDidReceiveSyncAck(p, remote, msg)
	case wire.MsgTypePush:
		DefaultDidReceivePush(p, remote, msg)
	case wire.MsgTypePushAck:
		DefaultDidReceivePushAck(p, remote, msg)
	case wire.MsgTypePull:
		DefaultDidReceivePull(p, remote, msg)
	case wire.MsgTypePullAck:
		DefaultDidReceivePullAck(p, remote, msg)
	default:
		p.opts.Logger.Error("listener", zap.String("message type", "invalid"))
	}
}

func DefaultDidReceiveSync(p *Peer, remote id.Signatory, msg wire.Msg) {
	fmt.Println("Ping")
}

func DefaultDidReceiveSyncAck(p *Peer, remote id.Signatory, msg wire.Msg) {
	fmt.Println("PingAck")
}

func DefaultDidReceivePush(p *Peer, remote id.Signatory, msg wire.Msg) {
	fmt.Println("Push message received")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	response := wire.Msg{
		Type: wire.MsgTypePushAck,
		Data:    nil,
	}
	if err := p.Send(ctx, remote, response); err != nil {
		p.opts.Logger.Error("gossip", zap.NamedError("pushAck", err))
		return
	}

	hash := [32]byte{}
	copy(hash[:], msg.Data[:])

	if _, ok := p.contentTable.Content(hash, uint8(wire.MsgTypePush)); !ok {
		response = wire.Msg{
			Type: wire.MsgTypePull,
			Data:    hash[:],
		}

		p.contentTable.Insert(hash, uint8(wire.MsgTypePull), nil)
		if err := p.Send(ctx, remote, response); err != nil {
			p.opts.Logger.Error("gossip", zap.NamedError("pull", err))
			p.contentTable.Delete(hash, uint8(wire.MsgTypePull))
			return
		}

	}
}

func DefaultDidReceivePushAck(p *Peer, remote id.Signatory, msg wire.Msg) {
	fmt.Println("PushAck received")
}

func DefaultDidReceivePull(p *Peer, remote id.Signatory, msg wire.Msg) {
	fmt.Println("Pull request incoming")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hash := [32]byte{}
	copy(hash[:], msg.Data[:])
	if data, ok := p.contentTable.Content(hash, uint8(wire.MsgTypePush)); ok {
		response := wire.Msg{
			Type: wire.MsgTypePullAck,
			Data:    data,
		}

		if err := p.Send(ctx, remote, response); err != nil {
			p.opts.Logger.Error("gossip", zap.NamedError("pull", err))
		}
		return
	}

	p.opts.Logger.Error("gossip", zap.String("pull request", "data not present"))

}

func DefaultDidReceivePullAck(p *Peer, remote id.Signatory, msg wire.Msg) {
	hash := id.NewHash(msg.Data)

	if _, ok := p.contentTable.Content(hash, uint8(wire.MsgTypePush)); !ok {
		p.opts.Logger.Error("gossip", zap.String("pull ack", "illegal data received"))
		return
	}
	p.contentTable.Delete(hash, uint8(wire.MsgTypePull))

	fmt.Printf("message forwarded from %v: %v\n", remote, string(msg.Data))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Gossip(ctx, id.Hash{}, msg.Data)
}
