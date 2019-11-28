package aw

import (
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
)

const (
	V1        = protocol.V1
	Ping      = protocol.Ping
	Pong      = protocol.Pong
	Cast      = protocol.Cast
	Multicast = protocol.Multicast
	Broadcast = protocol.Broadcast
)

type (
	Message          = protocol.Message
	MessageOnTheWire = protocol.MessageOnTheWire
	MessageLength    = protocol.MessageLength
	MessageVariant   = protocol.MessageVariant
	MessageVersion   = protocol.MessageVersion
	MessageSender    = protocol.MessageSender
	MessageReceiver  = protocol.MessageReceiver

	Event         = protocol.Event
	EventSender   = protocol.EventSender
	EventReceiver = protocol.EventReceiver

	Peer             = peer.Peer
	PeerOption       = peer.Options
	PeerID           = protocol.PeerID
	PeerIDs          = protocol.PeerIDs
	PeerAddress      = protocol.PeerAddress
	PeerAddresses    = protocol.PeerAddresses
	PeerAddressCodec = protocol.PeerAddressCodec

	SignVerifier = protocol.SignVerifier
	Handshaker   = handshake.Handshaker
	DHT          = dht.DHT
	Client       = protocol.Client
	Server       = protocol.Server

	TcpConnPoolOption = tcp.ConnPoolOptions
	TcpServerOption   = tcp.ServerOptions
)

var (
	NewPeer    = peer.New
	NewTcpPeer = peer.NewTCP
	NewDHT     = dht.New
)
