package peer

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"

	"go.uber.org/zap"
)

type SignatoryAndAddress struct {
	Signatory id.Signatory
	Addr      string
}

func (sigAndAddr SignatoryAndAddress) Marshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := surge.Marshal(sigAndAddr.Signatory, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal signatory: %v", err)
	}
	buf, rem, err = surge.MarshalString(sigAndAddr.Addr, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal address: %v", err)
	}
	return buf, rem, err
}
func (sigAndAddr SignatoryAndAddress) UnMarshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := surge.Unmarshal(&sigAndAddr.Signatory, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal signatory: %v", err)
	}
	buf, rem, err = surge.Unmarshal(&sigAndAddr.Addr, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal address: %v", err)
	}
	return buf, rem, err
}

type Ping func(context context.Context, maxExpectedPeers int) error

func PeerDisovery(self id.Signatory, addr string, logger *zap.Logger, t *transport.Transport, contentResolver dht.ContentResolver, addressTable dht.Table, alpha int, next Callbacks) (Callbacks, Ping) {

	discover := func(ctx context.Context, maxExpectedPeers int) error {

		if maxExpectedPeers <= 0  {
			return fmt.Errorf("max expected peers cannot be less than 1")
		}

		var expNumPeerBuf [8]byte
		binary.LittleEndian.PutUint64(expNumPeerBuf[:], uint64(maxExpectedPeers))
		msg := wire.Msg{
			Version: wire.MsgVersion1,
			Type:    wire.MsgTypePing,
			Data: 	 expNumPeerBuf[:],
		}

		for _, x := range addressTable.RandomAddresses(alpha) {
			addr, ok := addressTable.PeerAddress(x)
			if ok {
				msg.To = id.Hash(x)
				err := t.Send(ctx, x, addr, msg)
				if err != nil {
					logger.Debug("pinging", zap.Error(err))
				}
			} else {
				logger.Debug("pinging", zap.String("peer", "does not exist in table"))
			}
		}

		return nil
	}

	didReceivePing := func(from id.Signatory, msg wire.Msg) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var addressBuf [1024]byte
		addressByteSlice := addressBuf[:0]
		expNumPeer := int(binary.LittleEndian.Uint64(msg.To[:]))
		for _, sig := range addressTable.RandomAddresses(expNumPeer) {
			addr, ok := addressTable.PeerAddress(sig)
			if !ok {
				logger.Debug("pingAck", zap.String("peer", "does not exist in table"))
				addressTable.DeletePeer(sig)
			}
			addressByteSlice = append(addressByteSlice[:], []byte(addr)...)
			addressByteSlice = append(addressByteSlice, ' ')
		}
		response := wire.Msg{
			Version: wire.MsgVersion1,
			Type:    wire.MsgTypePingAck,
			To:      id.Hash(from),
			Data:    addressByteSlice,
		}
		addr, ok := addressTable.PeerAddress(from)
		if !ok {
			logger.Debug("pingAck", zap.String("peer", "does not exist in table"))
		}
		err := t.Send(ctx, from, addr, response)
		logger.Debug("pingAck", zap.Error(err))
	}

	didReceivePingAck := func(from id.Signatory, msg wire.Msg) {

	}

	return Callbacks{
		DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
			switch msg.Type {
			case wire.MsgTypePing:
				didReceivePing(from, msg)
			case wire.MsgTypePingAck:
				didReceivePingAck(from, msg)
			}

			if next.DidReceiveMessage != nil {
				next.DidReceiveMessage(from, msg)
			}
		},
	},
	discover
}
