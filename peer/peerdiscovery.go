package peer

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	"go.uber.org/zap"
)

type SignatoryAndAddress struct {
	Signatory id.Signatory
	Address   wire.Address
}

func (sigAndAddr SignatoryAndAddress) Marshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := sigAndAddr.Signatory.Marshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal signatory: %v", err)
	}
	buf, rem, err = sigAndAddr.Address.Marshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal address: %v", err)
	}
	return buf, rem, err
}
func (sigAndAddr *SignatoryAndAddress) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := (&sigAndAddr.Signatory).Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal signatory: %v", err)
	}
	buf, rem, err = sigAndAddr.Address.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal address: %v", err)
	}
	return buf, rem, err
}

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
	for {
		for _, sig := range dc.transport.Table().Peers(dc.opts.Alpha) {
			msg.To = id.Hash(sig)
			err := dc.transport.Send(ctx, sig, msg)
			if err != nil {
				dc.opts.Logger.Debug("pinging", zap.Error(err))
				dc.transport.Table().DeletePeer(sig)
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

	var addressBuf [1024]byte
	addressByteSlice := addressBuf[:0]
	port := binary.LittleEndian.Uint16(msg.Data)
	ipAddr, _ := dc.transport.Table().IP(from)
	dc.transport.Table().AddPeer(
		from,
		wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", ipAddr, port), uint64(time.Now().UnixNano())),
		)

	peers := dc.transport.Table().Peers(dc.opts.MaxExpectedPeers)
	for _, sig := range peers {
		var addrAndSig [128]byte
		addrAndSigSlice := addrAndSig[:]
		addr, ok := dc.transport.Table().PeerAddress(sig)
		if !ok {
			dc.opts.Logger.Debug("sending pingAck", zap.String("peer", "does not exist in table"))
			dc.transport.Table().DeletePeer(sig)
		}
		sigAndAddr := SignatoryAndAddress{Signatory: sig, Address: addr}
		tail, _, err := sigAndAddr.Marshal(addrAndSigSlice, len(addrAndSigSlice))
		if err != nil {
			dc.opts.Logger.Debug("sending pingAck", zap.Error(err))
		}
		addressByteSlice = append(addressByteSlice, addrAndSigSlice[:len(addrAndSigSlice)-len(tail)]...)
	}
	response := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePingAck,
		To:      id.Hash(from),
		Data:    addressByteSlice,
	}
	err := dc.transport.Send(ctx, from, response)
	if err != nil {
		dc.opts.Logger.Debug("sending pingAck", zap.Error(err))
	}
	return nil
}

func (dc *DiscoveryClient) didReceivePingAck(from id.Signatory, msg wire.Msg) error {
	var sigAndAddr SignatoryAndAddress
	var sigAndAddrArray [50]SignatoryAndAddress
	sigAndAddrSlice := sigAndAddrArray[:0]

	if cap(sigAndAddrArray) > dc.opts.MaxExpectedPeers {
		sigAndAddrSlice = make([]SignatoryAndAddress, 0, dc.opts.MaxExpectedPeers)
	}

	dataLeft := msg.Data
	count := 0
	for len(dataLeft) > 0 {
		count++
		if count > dc.opts.MaxExpectedPeers {
			return nil
		}
		tail, _, err := sigAndAddr.Unmarshal(dataLeft, len(dataLeft))
		dataLeft = tail
		if err != nil {
			dc.opts.Logger.Debug("receiving pingAck ", zap.Error(err))
			break
		}

		sigAndAddrSlice = append(sigAndAddrSlice, sigAndAddr)
		sigAndAddr = SignatoryAndAddress{}
	}

	for _, x := range sigAndAddrSlice {
		dc.transport.Table().AddPeer(x.Signatory, x.Address)
	}
	return nil
}
