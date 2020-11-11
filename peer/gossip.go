package peer

import (
	"context"
	"fmt"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type gossipFunc func(ctx context.Context, subnet id.Hash, contentID, content []byte) error

// Gossiper returns a new set of callback functions and a gossip function that can be used to spread information throughout a network

func Gossiper(t *transport.Transport, contentResolver dht.ContentResolver, addressTable Table, logger zap.Logger, next Callbacks) (Callbacks, gossipFunc) {

	gossip := func(ctx context.Context, subnet id.Hash, contentID, content []byte) error {
		msg := wire.Msg{Version: wire.MsgVersion1, Type: wire.MsgTypePush, Data: contentID}

		var chainedError error = nil
		for _, sig := range addressTable.Subnet() {
			addr, ok := addressTable.PeerAddress(sig)
			if !ok {
				logger.Error("gossip", zap.String("table", "peer not found"))
				continue
			}
			if err := t.Send(ctx, sig, addr, msg); err != nil {
				if chainedError == nil {
					chainedError = fmt.Errorf("%v, gossiping to peer %v : %v", chainedError, sig, err)
				} else {
					chainedError = fmt.Errorf("gossiping to peer %v : %v", sig, err)
				}
			}
		}
		return chainedError
	}

	didReceiveSync := func(from id.Signatory, msg wire.Msg) {
	}
	didReceiveSyncAck := func(from id.Signatory, msg wire.Msg) {
	}
	didReceivePush := func(from id.Signatory, msg wire.Msg) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		response := wire.Msg{
			Type: wire.MsgTypePushAck,
			Data: nil,
		}

		addr, ok := addressTable.PeerAddress(from)
		if !ok {
			logger.Error("gossip", zap.String("table", "peer not found"))
			return
		}
		if err := t.Send(ctx, from, addr, response); err != nil {
			logger.Error("gossip", zap.NamedError("pushAck", err))
			return
		}

		hash := [32]byte{}
		copy(hash[:], msg.Data[:])

		if _, ok := contentResolver.Content(hash, uint8(wire.MsgTypePush)); !ok {
			response = wire.Msg{
				Type: wire.MsgTypePull,
				Data: hash[:],
			}

			contentResolver.Insert(hash, uint8(wire.MsgTypePull), nil)
			if err := t.Send(ctx, from, addr, response); err != nil {
				logger.Error("gossip", zap.NamedError("pull", err))
				contentResolver.Delete(hash, uint8(wire.MsgTypePull))
				return
			}

		}
	}
	didReceivePushAck := func(from id.Signatory, msg wire.Msg) {
	}
	didReceivePull := func(from id.Signatory, msg wire.Msg) {
		hash := [32]byte{}
		copy(hash[:], msg.Data[:])
		if data, ok := contentResolver.Content(hash, uint8(wire.MsgTypePush)); ok {
			response := wire.Msg{
				Type: wire.MsgTypePullAck,
				Data: data,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			addr, ok := addressTable.PeerAddress(from)
			if !ok {
				logger.Error("gossip", zap.String("table", "peer not found"))
				return
			}
			if err := t.Send(ctx, from, addr, response); err != nil {
				logger.Error("gossip", zap.NamedError("pull", err))
			}
			return
		}

		logger.Error("gossip", zap.String("pull request", "data not present"))
	}
	didReceivePullAck := func(from id.Signatory, msg wire.Msg) {
		hash := id.NewHash(msg.Data)

		if _, ok := contentResolver.Content(hash, uint8(wire.MsgTypePush)); !ok {
			logger.Error("gossip", zap.String("pull ack", "illegal data received"))
			return
		}
		contentResolver.Delete(hash, uint8(wire.MsgTypePull))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gossip(ctx, id.Hash{}, hash[:], msg.Data)
	}

	return Callbacks{
			DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
				switch msg.Type {
				case wire.MsgTypeSync:
					didReceiveSync(from, msg)
				case wire.MsgTypeSyncAck:
					didReceiveSyncAck(from, msg)
				case wire.MsgTypePush:
					didReceivePush(from, msg)
				case wire.MsgTypePushAck:
					didReceivePushAck(from, msg)
				case wire.MsgTypePull:
					didReceivePull(from, msg)
				case wire.MsgTypePullAck:
					didReceivePullAck(from, msg)
				default:
					if next.DidReceiveMessage != nil {
						next.DidReceiveMessage(from, msg)
						break
					}
					logger.Error("listener", zap.String("invalid message type", fmt.Sprint(msg.Type)))
				}
			},
		},
		gossip


}
