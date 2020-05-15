package peer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"math"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type Peer struct {
	opts Options

	dht     dht.DHT
	trans   *transport.Transport
	privKey *ecdsa.PrivateKey
}

func New(opts Options, dht dht.DHT, trans *transport.Transport, privKey *ecdsa.PrivateKey) *Peer {

	// Sign the address. This address will be the address that the we will use
	// to let other Peers know we exist. If we cannot sign the address, then we
	// log a fatal error.
	if err := opts.Addr.Sign(privKey); err != nil {
		opts.Logger.Fatalf("signing address=%v: %v", opts.Addr, err)
	}

	// Given the number of parallel pings that can happen, the number of pings
	// we will attempt at any one time, and the maximum time that a ping can
	// take, we check that the interval between rounds of pinging is
	// sufficiently long that we will not be attempting the next round of
	// pinging before we have finished the previous round. We use a factor of 2,
	// to make sure there is plenty of margin for error.
	expectedPingInterval := 2 * time.Duration(math.Ceil(float64(opts.Alpha)/float64(opts.NumBackgroundWorkers))) * opts.PingTimeout
	if opts.PingInterval < expectedPingInterval {
		opts.Logger.Warnf("ping interval is too low: expected>=%v, got=%v", expectedPingInterval, opts.PingInterval)
	}

	return &Peer{
		opts: opts,

		dht:     dht,
		trans:   trans,
		privKey: privKey,
	}
}

// Run the Peer. This will periodically attempt to Ping random addresses in the
// DHT. If the Ping is not successful, the address will be removed from the DHT.
func (peer *Peer) Run(ctx context.Context) {
	peer.opts.Logger.Infof("peering with address=%v", peer.opts.Addr)

	// Start by pinging some random addresses from the DHT.
	peer.Ping(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(peer.opts.PingInterval):
			// Ping periodically.
			peer.Ping(ctx)
		}
	}
}

// Identity returns the signatory of the peer.
func (peer *Peer) Identity() id.Signatory {
	return id.NewSignatory(&peer.privKey.PublicKey)
}

// Addr returns the network address of this peer.
func (peer *Peer) Addr() wire.Address {
	return peer.opts.Addr
}

// Ping a number of random Peers from the DHT. If the Peer is not responsive,
// then it will be removed from the DHT.
func (peer *Peer) Ping(ctx context.Context) {
	// We use this local type to couple signatories with an address. This is
	// useful for performance, since we can reduce the amount of times we have
	// to explicitly compute the signatory from the address.
	type SignatoryAndAddress struct {
		Signatory id.Signatory
		Addr      wire.Address
	}

	// Marshal our own address, so that we can ping it to other Peers.
	marshaledPing, err := surge.ToBinary(wire.PingV1{Addr: peer.opts.Addr})
	if err != nil {
		peer.opts.Logger.Fatalf("marshaling ping: %v", err)
	}
	ping := wire.Message{
		Version: wire.V1,
		Type:    wire.Ping,
		Data:    marshaledPing,
	}

	// Create background workers so that we only consume a bounded number of
	// goroutines to perform the round of pinging. We create the workers
	// on-demand, because we generally do not expect pinging to be done often.
	queue := make(chan SignatoryAndAddress, 2*peer.opts.Alpha)
	for i := 0; i < peer.opts.NumBackgroundWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case signatoryAndAddr := <-queue:
					peer.opts.Logger.Debugf("dequeuing signatory=%v at address=%v", signatoryAndAddr.Signatory, signatoryAndAddr.Addr)

					// Ping the address with our own address. If the Ping
					// returns an error, then we remove the address from the
					// DHT.
					func() {
						innerCtx, innerCancel := context.WithTimeout(ctx, peer.opts.PingTimeout)
						defer innerCancel()
						if err := peer.trans.Send(innerCtx, signatoryAndAddr.Addr, ping); err != nil {
							peer.opts.Logger.Warnf("pinging address=%v: %v", signatoryAndAddr.Addr, err)
							peer.opts.Logger.Infof("deleting address=%v", signatoryAndAddr.Addr)
							peer.dht.DeleteAddr(signatoryAndAddr.Signatory)
						}
					}()
				}
			}
		}()
	}

	// Grab some random addresses from the DHT and add them to the queue for
	// pinging.
	addrsBySignatory := peer.dht.Addrs(peer.opts.Alpha)
	for signatory, addr := range addrsBySignatory {
		select {
		case <-ctx.Done():
			return
		case queue <- SignatoryAndAddress{Signatory: signatory, Addr: addr}:
			peer.opts.Logger.Debugf("pinging signatory=%v at address=%v", signatory, addr)
		}
	}
}

func (peer *Peer) DidReceivePing(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if version != wire.V1 {
		return wire.Message{}, fmt.Errorf("unsupported version=%v", version)
	}

	remoteAddr := wire.Address{}
	if err := surge.FromBinary(data, &remoteAddr); err != nil {
		return wire.Message{}, fmt.Errorf("unsupported remote address: %v", err)
	}

	if peer.dht.InsertAddr(from, remoteAddr) {
		peer.opts.Logger.Infof("peer found with remote address=%v", remoteAddr)
	}

	addrsBySignatory := peer.dht.Addrs(peer.opts.Alpha)
	pingAckV1 := wire.PingAckV1{
		Addrs: make([]wire.Address, 0, len(addrsBySignatory)+1),
	}
	pingAckV1.Addrs = append(pingAckV1.Addrs, peer.opts.Addr)
	for _, addr := range addrsBySignatory {
		pingAckV1.Addrs = append(pingAckV1.Addrs, addr)
	}
	marshaledPingAckV1, err := surge.ToBinary(pingAckV1)
	if err != nil {
		// Being unable to marshal addresses is a fatal error. We should never
		// admit addresses into the DHT that are invalid, or that cannot be
		// marshaled.
		peer.opts.Logger.Fatalf("marshaling ping ack: %v", len(pingAckV1.Addrs), err)
	}

	// Send a ping acknowledgemet, containing our own address, to the address in
	// the ping. This may not be the same peer that sent us the ping, so we will
	// also return this acknowledgement.
	pingAck := wire.Message{
		Version: wire.V1,
		Type:    wire.PingAck,
		Data:    marshaledPingAckV1,
	}
	return pingAck, nil
}

func (peer *Peer) DidReceivePingAck(version uint8, data []byte, from id.Signatory) error {
	if version != wire.V1 {
		return fmt.Errorf("unsupported version=%v", version)
	}

	// Unmarshal the remote addresses returned to us in the PingAck response,
	// and limit the number of addreses to prevent spam.
	pingAckV1 := wire.PingAckV1{}
	if err := surge.FromBinary(data, &pingAckV1); err != nil {
		return fmt.Errorf("unsupported remote address: %v", err)
	}
	if len(pingAckV1.Addrs) > peer.opts.Alpha {
		pingAckV1.Addrs = pingAckV1.Addrs[:peer.opts.Alpha]
	}

	// Prepare a slice for all of the new addresses that we see for the first
	// time.
	newRemoteAddrs := make([]wire.Address, 0, len(pingAckV1.Addrs))

	// Add all of the remote addresses to the DHT and keep track of the
	// addresses that we are seeing for the first time.
	buf := new(bytes.Buffer)
	for _, addr := range pingAckV1.Addrs {
		buf.Reset()
		remoteSignatory, err := addr.SignatoryWithBuffer(buf)
		if err != nil {
			peer.opts.Logger.Warnf("identifying remote address=%v: %v", addr, err)
			continue
		}
		if peer.dht.InsertAddr(remoteSignatory, addr) {
			newRemoteAddrs = append(newRemoteAddrs, addr)
		}
	}

	return nil
}
