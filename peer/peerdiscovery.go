package peer

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/renproject/surge"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	"go.uber.org/zap"
)

type SignatoryAndAddress struct {
	Signatory id.Signatory
	Address      string
}

func (sigAndAddr SignatoryAndAddress) Marshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := surge.Marshal(sigAndAddr.Signatory, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal signatory: %v", err)
	}
	buf, rem, err = surge.MarshalString(sigAndAddr.Address, buf, rem)
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
	buf, rem, err = surge.Unmarshal(&sigAndAddr.Address, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal address: %v", err)
	}
	return buf, rem, err
}

type Ping func(context context.Context)

func PeerDisovery(t *transport.Transport, addressTable dht.Table, logger *zap.Logger, alpha int, maxExpectedPeers int, pingTimePeriod time.Duration, next Callbacks) (Callbacks, Ping) {

	discover := func(ctx context.Context) {
		if maxExpectedPeers <= 0  {
			logger.Debug("pinging", zap.String("max expected peers cannot be less than 1", "setting to default - 5"))
			maxExpectedPeers = 5
		}

		go func() {
			var expNumPeerBuf [8]byte
			for {
				binary.LittleEndian.PutUint64(expNumPeerBuf[:], uint64(maxExpectedPeers))
				msg := wire.Msg{
					Version: wire.MsgVersion1,
					Type:    wire.MsgTypePing,
					Data:    expNumPeerBuf[:],
				}

				for _, sig := range addressTable.RandomAddresses(alpha) {
					addr, ok := addressTable.PeerAddress(sig)
					if ok {
						msg.To = id.Hash(sig)
						err := t.Send(ctx, sig, addr, msg)
						if err != nil {
							logger.Debug("pinging", zap.Error(err))
							addressTable.DeletePeer(sig)
						}
					} else {
						logger.Debug("pinging", zap.String("peer", "does not exist in table"))
						addressTable.DeletePeer(sig)
					}
				}
				<-time.After(pingTimePeriod)
			}
		}()
	}

	didReceivePing := func(from id.Signatory, msg wire.Msg) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var addressBuf [1024]byte
		addressByteSlice := addressBuf[:0]
		expNumPeer := int(binary.LittleEndian.Uint64(msg.To[:]))
		for _, sig := range addressTable.RandomAddresses(expNumPeer) {
			var addrAndSigBig [128]byte
			addrAndSigBigSlice := addrAndSigBig[:]
			addr, ok := addressTable.PeerAddress(sig)
			if !ok {
				logger.Debug("sending pingAck", zap.String("peer", "does not exist in table"))
				addressTable.DeletePeer(sig)
			}
			sigAndAddr := SignatoryAndAddress{Signatory: sig, Address: addr}
			tail, rem, err := sigAndAddr.Marshal(addrAndSigBigSlice, len(addrAndSigBigSlice))
			if err != nil {
				logger.Debug("sending pingAck", zap.Error(err))
			}
			addressByteSlice = append(addressByteSlice, tail[:len(tail)-rem]...)
		}
		response := wire.Msg{
			Version: wire.MsgVersion1,
			Type:    wire.MsgTypePingAck,
			To:      id.Hash(from),
			Data:    addressByteSlice,
		}
		addr, ok := addressTable.PeerAddress(from)
		if !ok {
			logger.Debug("sending pingAck", zap.String("peer", "does not exist in table"))
		}
		err := t.Send(ctx, from, addr, response)
		if err != nil {
			logger.Debug("sending pingAck", zap.Error(err))
		}
	}

	didReceivePingAck := func(from id.Signatory, msg wire.Msg) {
		rem := len(msg.Data)

		var sigAndAddr SignatoryAndAddress
		for rem > 0 {
			_, _, err := sigAndAddr.UnMarshal(msg.Data, len(msg.Data))
			if err != nil {
				logger.Debug("receiving pingAck ", zap.Error(err))
				break
			}
			addressTable.AddPeer(sigAndAddr.Signatory, sigAndAddr.Address)
			sigAndAddr = SignatoryAndAddress{}
		}

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
