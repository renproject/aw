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
	// Messages
	Message          = protocol.Message
	MessageOnTheWire = protocol.MessageOnTheWire
	MessageLength    = protocol.MessageLength
	MessageVariant   = protocol.MessageVariant
	MessageVersion   = protocol.MessageVersion
	MessageBody      = protocol.MessageBody
	MessageSender    = protocol.MessageSender
	MessageReceiver  = protocol.MessageReceiver

	// Events
	Event                = protocol.Event
	EventSender          = protocol.EventSender
	EventReceiver        = protocol.EventReceiver
	EventPeerChanged     = protocol.EventPeerChanged
	EventMessageReceived = protocol.EventMessageReceived

	// Peers
	Peer             = peer.Peer
	PeerOptions      = peer.Options
	PeerID           = protocol.PeerID
	PeerIDs          = protocol.PeerIDs
	GroupID          = protocol.GroupID
	PeerAddress      = protocol.PeerAddress
	PeerAddresses    = protocol.PeerAddresses
	PeerAddressCodec = protocol.PeerAddressCodec

	// Network
	DHT            = dht.DHT
	Client         = protocol.Client
	Server         = protocol.Server
	Session        = protocol.Session
	SessionManager = protocol.SessionManager
	SignVerifier   = protocol.SignVerifier
	Handshaker     = handshake.Handshaker

	// Options
	TCPConnPoolOptions = tcp.ConnPoolOptions
	TCPServerOptions   = tcp.ServerOptions
)

// Default values
var NilGroupID = protocol.NilGroupID

// Constructors
var (
	NewMessage   = protocol.NewMessage
	NewPeer      = peer.New
	NewDHT       = dht.New
	NewTCPPeer   = peer.NewTCP
	NewConnPool  = tcp.NewConnPool
	NewTCPClient = tcp.NewClient
	NewTCPServer = tcp.NewServer
)
