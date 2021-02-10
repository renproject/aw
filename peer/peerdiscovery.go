package peer

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/renproject/surge"
	"time"

	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	"go.uber.org/zap"
)

type DiscoveryClient struct {
	opts DiscoveryOptions

	transport *transport.Transport
}

func NewDiscoveryClient(opts DiscoveryOptions, transport *transport.Transport) *DiscoveryClient {
	return &DiscoveryClient{
		opts: opts,
		transport: transport,
	}
}

func (dc *DiscoveryClient) DiscoverPeers(ctx context.Context) {
	var pingData [2]byte
	binary.LittleEndian.PutUint16(pingData[:], dc.transport.Port())

	msg := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePing,
		Data:    pingData[:],
	}

	ticker := time.NewTicker(dc.opts.PingTimePeriod)
	defer ticker.Stop()

	alpha := dc.opts.Alpha
	sendDuration := dc.opts.PingTimePeriod/time.Duration(alpha)
	outer:
	for {
		for _, sig := range dc.transport.Table().Peers(alpha) {
			innerCtx, cancel := context.WithTimeout(ctx, sendDuration)
			msg.To = id.Hash(sig)
			err := dc.transport.Send(innerCtx, sig, msg)
			cancel()
			if err != nil {
				dc.opts.Logger.Debug("pinging", zap.Error(err))
				if err == context.Canceled || err == context.DeadlineExceeded {
					break
				}
			}
			select {
			case <-ticker.C:
				continue outer
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (dc *DiscoveryClient) DidReceiveMessage(from id.Signatory, msg wire.Msg) error {
	switch msg.Type {
	case wire.MsgTypePing:
		if err := dc.didReceivePing(from, msg); err != nil {
			return err
		}
	case wire.MsgTypePingAck:
		if err := dc.didReceivePingAck(from, msg); err != nil {
			return err
		}
	}
	return nil
}

func (dc *DiscoveryClient) didReceivePing(from id.Signatory, msg wire.Msg) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if dataLen := len(msg.Data); dataLen != 2 {
		return fmt.Errorf("malformed port received in ping message. expected: 2 bytes, received: %v bytes", dataLen)
	}
	port := binary.LittleEndian.Uint16(msg.Data)
	ipAddr, ipAddrOk := dc.transport.Table().IP(from)
	if !ipAddrOk {
		if _, ok := dc.transport.Table().PeerAddress(from); ok {
			return nil
		}
		return fmt.Errorf("ip address for remote peer not found")
	}
	dc.transport.Table().AddPeer(
		from,
		wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", ipAddr, port), uint64(time.Now().UnixNano())),
	)
	dc.transport.Table().DeleteIP(from)

	peers := dc.transport.Table().Peers(dc.opts.MaxExpectedPeers)
	addrAndSig := make([]wire.SignatoryAndAddress, 0, len(peers))
	for _, sig := range peers {
		addr, ok := dc.transport.Table().PeerAddress(sig)
		if !ok {
			dc.opts.Logger.Debug("sending pingAck", zap.String("peer", "does not exist in table"))
		}
		sigAndAddr := wire.SignatoryAndAddress{Signatory: sig, Address: addr}
		addrAndSig = append(addrAndSig, sigAndAddr)
	}
	var addrAndSigBytes [2048]byte
	_, rem, err := surge.Marshal(&addrAndSig, addrAndSigBytes[:], len(addrAndSigBytes))
	if err != nil {
		dc.opts.Logger.Debug("sending pingAck ", zap.Error(err))
	}
	response := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePingAck,
		To:      id.Hash(from),
		Data:    addrAndSigBytes[:len(addrAndSigBytes)-rem],
	}

	if err := dc.transport.Send(ctx, from, response); err != nil {
		dc.opts.Logger.Debug("sending pingAck", zap.Error(err))
	}
	return nil
}

func (dc *DiscoveryClient) didReceivePingAck(from id.Signatory, msg wire.Msg) error {
	slice := []wire.SignatoryAndAddress{}
	_, _, err := surge.Unmarshal(&slice, msg.Data, surge.MaxBytes)
	if err != nil {
		dc.opts.Logger.Debug("receiving pingAck ", zap.Error(err))
		return fmt.Errorf("unmarshal error")
	}

	for _, x := range slice {
		dc.transport.Table().AddPeer(x.Signatory, x.Address)
	}
	return nil
}
