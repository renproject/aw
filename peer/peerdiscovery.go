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
	var maxExpectedPeers int
	if dc.opts.MaxExpectedPeers <= 0  {
		dc.opts.Logger.Debug("pinging", zap.String("max expected peers cannot be less than 1", "setting to default - 5"))
		maxExpectedPeers = 5
	} else {
		maxExpectedPeers = dc.opts.MaxExpectedPeers
	}

	go func() {
		var pingData [16]byte
		binary.LittleEndian.PutUint64(pingData[:8], uint64(maxExpectedPeers))
		binary.LittleEndian.PutUint64(pingData[8:], uint64(dc.transport.Port()))

		msg := wire.Msg{
			Version: wire.MsgVersion1,
			Type:    wire.MsgTypePing,
			Data:    pingData[:],
		}
		for {
			for _, sig := range dc.transport.Table().Peers(dc.opts.Alpha) {
				msg.To = id.Hash(sig)
				err := dc.transport.Send(ctx, sig, msg)
				if err != nil {
					dc.opts.Logger.Debug("pinging", zap.Error(err))
					dc.transport.Table().DeletePeer(sig)
				}
			}
			<-time.After(dc.opts.PingTimePeriod)
		}
	}()
}

func (dc *DiscoveryClient) DidReceiveMessage(from id.Signatory, msg wire.Msg) {
	switch msg.Type {
	case wire.MsgTypePing:
		dc.didReceivePing(from, msg)
	case wire.MsgTypePingAck:
		dc.didReceivePingAck(from, msg)
	}
}

func (dc *DiscoveryClient) didReceivePing(from id.Signatory, msg wire.Msg) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var addressBuf [1024]byte
	addressByteSlice := addressBuf[:0]
	expNumPeer := int(binary.LittleEndian.Uint64(msg.Data[:8]))
	port := uint16(binary.LittleEndian.Uint64(msg.Data[8:]))
	ipAddr, _ := dc.transport.Table().IP(from)
	dc.transport.Table().AddPeer(
		from,
		wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", ipAddr, port), uint64(time.Now().UnixNano())),
		)

	peers := dc.transport.Table().Peers(expNumPeer)
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
}

func (dc *DiscoveryClient) didReceivePingAck(from id.Signatory, msg wire.Msg) {
	var sigAndAddr SignatoryAndAddress
	dataLeft := msg.Data
	for len(dataLeft) > 0 {
		tail, _, err := sigAndAddr.Unmarshal(dataLeft, len(dataLeft))
		dataLeft = tail
		if err != nil {
			dc.opts.Logger.Debug("receiving pingAck ", zap.Error(err))
			break
		}

		dc.transport.Table().AddPeer(sigAndAddr.Signatory, sigAndAddr.Address)
		sigAndAddr = SignatoryAndAddress{}
	}
}
