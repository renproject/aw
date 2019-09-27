package tcp

import (
	"context"
	"net"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

// ClientOptions define how the Client manages its connections with remote
// servers. It also defines some other simple behaviours, such as logging.
type ClientOptions struct {
	// Logger is used to log information and errors.
	Logger logrus.FieldLogger
	// Timeout after which the Client will stop an attempt to dial a remote
	// server.
	Timeout time.Duration
	// MaxConnections to remote servers that the Client will maintain.
	MaxConnections int
	// Handshaker handles the handshake process between peers. Default: no handshake
	Handshaker handshake.Handshaker
}

func (options *ClientOptions) setZerosToDefaults() {
	if options.Logger == nil {
		options.Logger = logrus.New()
	}
	if options.Timeout == time.Duration(0) {
		options.Timeout = 5 * time.Second
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = 512
	}
}

type Client struct {
	options  ClientOptions
	pool     ConnPool
	dht      dht.DHT
	messages protocol.MessageReceiver
}

func NewClient(options ClientOptions, pool ConnPool, dht dht.DHT, messages protocol.MessageReceiver) *Client {
	options.setZerosToDefaults()
	return &Client{
		options:  options,
		pool:     pool,
		dht:      dht,
		messages: messages,
	}
}

func (client *Client) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageOtw := <-client.messages:
			client.handleMessageOnTheWire(messageOtw)
		}
	}
}

func (client *Client) handleMessageOnTheWire(message protocol.MessageOnTheWire) {
	netAddrs, err := client.netAddrs(message)
	if err != nil {
		client.options.Logger.Errorf("error loading network addresses: %v", err)
	}

	for _, netAddr := range netAddrs {
		client.sendMessageOnTheWire(netAddr, message.Message)
	}
}

func (client *Client) sendMessageOnTheWire(to net.Addr, message protocol.Message) {
	if err := client.pool.Send(to, message); err != nil {
		client.options.Logger.Errorf("error writing to tcp connection to %v: %v", to.String(), err)
		return
	}
}

func (client *Client) netAddrs(message protocol.MessageOnTheWire) ([]net.Addr, error) {
	var peerAddrs protocol.PeerAddresses

	switch message.Message.Variant {
	case protocol.Broadcast, protocol.Multicast:
		peerAddresses, err := client.dht.PeerAddresses()
		if err != nil {
			return nil, err
		}
		peerAddrs = peerAddresses

	case protocol.Cast, protocol.Ping, protocol.Pong:
		peerAddress, err := client.dht.PeerAddress(message.To)
		if err != nil {
			return nil, err
		}
		peerAddrs = protocol.PeerAddresses{peerAddress}

	default:
		return nil, protocol.NewErrMessageVariantIsNotSupported(message.Message.Variant)
	}

	netAddrs := make([]net.Addr, len(peerAddrs))
	for i, peerAddr := range peerAddrs {
		netAddrs[i] = peerAddr.NetworkAddress()
	}
	return netAddrs, nil
}
