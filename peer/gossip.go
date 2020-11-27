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

type GossipFunc func(ctx context.Context, subnet id.Hash, contentID []byte) error

// Gossiper returns a new set of callback functions and a gossip function that can be used to spread information throughout a network.
func Gossiper(logger *zap.Logger, t *transport.Transport, contentResolver dht.ContentResolver, addressTable dht.Table, alpha int, next Callbacks) (Callbacks, GossipFunc) {

	gossip := func(ctx context.Context, subnet id.Hash, contentID []byte) error {
		msg := wire.Msg{Version: wire.MsgVersion1, To: subnet, Type: wire.MsgTypePush, Data: contentID}

		var chainedError error = nil
		var receivers []id.Signatory
		if subnet.Equal(&GlobalSubnet) {
			receivers = addressTable.Addresses(alpha)
		} else {
			receivers = addressTable.Subnet(subnet)
		}
		
		for _, sig := range receivers {
			addr, ok := addressTable.PeerAddress(sig)
			if !ok {
				logger.Error("gossip", zap.String("table", "peer not found"))
				continue
			}
			if err := t.Send(ctx, sig, addr, msg); err != nil {
				if chainedError == nil {
					chainedError = fmt.Errorf("%v, gossiping to %v: %v", chainedError, sig, err)
				} else {
					chainedError = fmt.Errorf("gossiping to %v: %v", sig, err)
				}
			}
		}
		return chainedError
	}

	didReceivePush := func(from id.Signatory, msg wire.Msg) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		addr, ok := addressTable.PeerAddress(from)
		if !ok {
			logger.Error("gossip", zap.Error(ErrPeerNotFound))
			return
		}

		if _, ok := contentResolver.Content(msg.Data); !ok {
			response := wire.Msg{
				Version: wire.MsgVersion1,
				Type: wire.MsgTypePull,
				To: id.Hash(from),
				Data: msg.Data,
			}

			contentResolver.Insert(msg.Data, nil)
			if err := t.Send(ctx, from, addr, response); err != nil {
				contentResolver.Delete(msg.Data)
				logger.Error("gossip", zap.NamedError("pull", err))
				return
			}
		}
	}

	didReceivePull := func(from id.Signatory, msg wire.Msg) {
		if data, ok := contentResolver.Content(msg.Data); ok {
			response := wire.Msg{
				Version: wire.MsgVersion1,
				To : id.Hash(from),
				Type: wire.MsgTypeSync,
				Data: msg.Data,
				SyncData: data,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			addr, ok := addressTable.PeerAddress(from)
			if !ok {
				logger.Error("gossip", zap.String("table", "peer not found"))
				return
			}
			if err := t.Send(ctx, from, addr, response); err != nil {
				logger.Error("gossip", zap.NamedError("sending sync", err))
			}
			return
		}

		logger.Error("gossip", zap.String("pull request", "data not found"))
	}

	didReceiveSync := func(from id.Signatory, msg wire.Msg) {
		contentID := make(dht.ContentID, len(msg.Data))
		copy(contentID, msg.Data)
		content, ok := contentResolver.Content(contentID)
		if !ok {
			logger.Error("gossip", zap.String("receiving sync", "unknown id in sync message"))
			return
		}
		if content != nil {
			// TODO : Add debugging message for logger
			return
		}
		contentResolver.Insert(contentID, msg.SyncData)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := gossip(ctx, GlobalSubnet, contentID); err != nil {
			logger.Error("gossiping sync" , zap.Error(err))
		}
	}

	return Callbacks{
			DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
				switch msg.Type {
				case wire.MsgTypePush:
					didReceivePush(from, msg)
				case wire.MsgTypePull:
					didReceivePull(from, msg)
				case wire.MsgTypeSync:
					didReceiveSync(from, msg)
				case wire.MsgTypeSend:
				}

				if next.DidReceiveMessage != nil {
					next.DidReceiveMessage(from, msg)
				}
			},
		},
		gossip
}
