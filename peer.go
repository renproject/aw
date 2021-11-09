package aw

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/session"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	DefaultSubnet = id.Hash{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}

	DefaultMaxLinkedPeers               uint          = 100
	DefaultMaxEphemeralConnections      uint          = 20
	DefaultMaxPendingSyncs              uint          = 100
	DefaultMaxActiveSyncsForSameContent uint          = 10
	DefaultMaxGossipSubnets             uint          = 100 // ?
	DefaultMaxMessageSize               uint          = 1024
	DefaultOutgoingBufferSize           uint          = 100
	DefaultEventLoopBufferSize          uint          = 100
	DefaultOutgoingBufferTimeout        time.Duration = time.Second
	DefaultWriteTimeout                 time.Duration = time.Second
	DefaultDialRetryInterval            time.Duration = time.Second
	DefaultEphemeralConnectionTTL       time.Duration = 5 * time.Second
	DefaultMinimumConnectionExpiryAge   time.Duration = time.Minute
	DefaultGossipAlpha                  int           = 5
	DefaultGossipTimeout                time.Duration = 5 * time.Second // ?
	DefaultPingAlpha                    int           = 5
	DefaultPongAlpha                    int           = 10
	DefaultPeerDiscoveryInterval        time.Duration = 30 * time.Second
	DefaultPeerExpiryTimeout            time.Duration = 30 * time.Second
)

var (
	ErrTooManyLinkedPeers          = errors.New("too many linked peers")
	ErrTooManyEphemeralConnections = errors.New("too many ephemeral connections")
	ErrMessageBufferFull           = errors.New("outgoing message buffer is full")
	ErrEventLoopFull               = errors.New("event loop buffer is full")
	ErrTooManyPendingSyncs         = errors.New("too many pending sync requests")
	ErrTooManySyncsForSameContent  = errors.New("too many simultaneous syncs for the same content ID")
)

type EventType uint

const (
	IncomingMessage EventType = iota
	SendMessage
	GossipMessage
	SyncRequest
	ReaderDropped
	WriterDropped
	NewConnection
	KeepAlive
	DialTimeout
	LinkPeer
	DiscoverPeers
	UnlinkPeer
)

const (
	KeepAliveFalse byte = 0x00
	KeepAliveTrue  byte = 0x01
)

func (ty EventType) String() string {
	switch ty {
	case IncomingMessage:
		return "IncomingMessage"
	case SendMessage:
		return "SendMessage"
	case GossipMessage:
		return "GossipMessage"
	case SyncRequest:
		return "SyncRequest"
	case ReaderDropped:
		return "ReaderDropped"
	case WriterDropped:
		return "WriterDropped"
	case NewConnection:
		return "NewConnection"
	case KeepAlive:
		return "KeepAlive"
	case DialTimeout:
		return "DialTimeout"
	case LinkPeer:
		return "LinkPeer"
	case DiscoverPeers:
		return "DiscoverPeers"
	case UnlinkPeer:
		return "UnlinkPeer"
	default:
		return fmt.Sprintf("unknown(%v)", uint(ty))
	}
}

type Event struct {
	Type EventType

	ID         id.Signatory
	Subnet     id.Hash
	Hint       *id.Signatory
	Message    wire.Msg
	Addr       net.Addr
	Connection net.Conn
	GCMSession *session.GCMSession
	Ctx        context.Context
	Error      error

	MessageResponder chan<- []byte
	ErrorResponder   chan<- error
}

type PeerConnection struct {
	ReadDone  chan struct{}
	WriteDone chan *wire.Msg

	Connection       net.Conn
	GCMSession       *session.GCMSession
	Timestamp        time.Time
	OutgoingMessages chan wire.Msg
	PendingMessage   *wire.Msg
}

type EphemeralConnection struct {
	PeerConnection

	ExpiryDeadline time.Time
}

type PendingSync struct {
	Ctx        context.Context
	Responders []chan<- []byte
}

type GossipSubnet struct {
	Subnet id.Hash
	Expiry time.Time
}

type Options struct {
	Logger *zap.Logger

	MaxLinkedPeers               uint
	MaxEphemeralConnections      uint
	MaxPendingSyncs              uint
	MaxActiveSyncsForSameContent uint
	MaxGossipSubnets             uint
	MaxMessageSize               uint
	OutgoingBufferSize           uint
	EventLoopBufferSize          uint
	OutgoingBufferTimeout        time.Duration
	WriteTimeout                 time.Duration
	DialRetryInterval            time.Duration
	EphemeralConnectionTTL       time.Duration
	MinimumConnectionExpiryAge   time.Duration

	GossipAlpha   int
	GossipTimeout time.Duration

	PingAlpha             int
	PongAlpha             int
	PeerDiscoveryInterval time.Duration
	PeerExpiryTimeout     time.Duration

	ListenerOptions              ListenerOptions
	ConnectionRateLimiterOptions RateLimiterOptions
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return Options{
		Logger: logger,

		MaxLinkedPeers:               DefaultMaxLinkedPeers,
		MaxEphemeralConnections:      DefaultMaxEphemeralConnections,
		MaxPendingSyncs:              DefaultMaxPendingSyncs,
		MaxActiveSyncsForSameContent: DefaultMaxActiveSyncsForSameContent,
		MaxGossipSubnets:             DefaultMaxGossipSubnets,
		MaxMessageSize:               DefaultMaxMessageSize,
		OutgoingBufferSize:           DefaultOutgoingBufferSize,
		EventLoopBufferSize:          DefaultEventLoopBufferSize,
		OutgoingBufferTimeout:        DefaultOutgoingBufferTimeout,
		WriteTimeout:                 DefaultWriteTimeout,
		DialRetryInterval:            DefaultDialRetryInterval,
		EphemeralConnectionTTL:       DefaultEphemeralConnectionTTL,
		MinimumConnectionExpiryAge:   DefaultMinimumConnectionExpiryAge,

		GossipAlpha:   DefaultGossipAlpha,
		GossipTimeout: DefaultGossipTimeout,

		PingAlpha:             DefaultPingAlpha,
		PongAlpha:             DefaultPongAlpha,
		PeerDiscoveryInterval: DefaultPeerDiscoveryInterval,
		PeerExpiryTimeout:     DefaultPeerExpiryTimeout,

		ListenerOptions:              DefaultListenerOptions(),
		ConnectionRateLimiterOptions: DefaultRateLimiterOptions(),
	}
}

type Peer struct {
	Opts Options

	Self    id.Signatory
	PrivKey *id.PrivKey
	Port    uint16

	Receive func(id.Signatory, []byte)

	Ctx                  context.Context
	Events               chan Event
	LinkedPeers          map[id.Signatory]*PeerConnection
	EphemeralConnections map[id.Signatory]*EphemeralConnection
	PendingSyncs         map[string]PendingSync
	GossipSubnets        map[string]GossipSubnet

	PeerTable       dht.Table
	ContentResolver dht.ContentResolver
	Filter          *syncFilter
}

func New(opts Options, privKey *id.PrivKey, peerTable dht.Table, contentResolver dht.ContentResolver, receive func(id.Signatory, []byte)) *Peer {
	self := privKey.Signatory()

	events := make(chan Event, opts.EventLoopBufferSize)
	linkedPeers := make(map[id.Signatory]*PeerConnection, opts.MaxLinkedPeers)
	ephemeralConnections := make(map[id.Signatory]*EphemeralConnection, opts.MaxEphemeralConnections)
	pendingSyncs := make(map[string]PendingSync, opts.MaxPendingSyncs)
	gossipSubnets := make(map[string]GossipSubnet, opts.MaxGossipSubnets)

	filter := newSyncFilter()

	return &Peer{
		Opts: opts,

		Self:    self,
		PrivKey: privKey,
		Port:    0,

		Receive: receive,

		Ctx:                  nil,
		Events:               events,
		LinkedPeers:          linkedPeers,
		EphemeralConnections: ephemeralConnections,
		PendingSyncs:         pendingSyncs,
		GossipSubnets:        gossipSubnets,

		PeerTable:       peerTable,
		ContentResolver: contentResolver,
		Filter:          filter,
	}
}

func (peer *Peer) Listen(ctx context.Context, address string) (uint16, error) {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", address)
	if err != nil {
		return 0, err
	}

	peer.Port = uint16(listener.Addr().(*net.TCPAddr).Port)

	// The 'ctx' we passed to Listen() will not unblock `Listener.Accept()` if
	// context exceeding the deadline. We need to manually close the listener
	// to stop `Listener.Accept()` from blocking.
	// See https://github.com/golang/go/issues/28120
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	go listen(ctx, listener, peer.listenerHandler, peer.Opts.ListenerOptions, peer.Opts.Logger)

	return peer.Port, nil
}

func (peer *Peer) Run(ctx context.Context) error {
	peer.Ctx = ctx

	// Peer discovery.
	go func() {
		ticker := time.NewTicker(peer.Opts.PeerDiscoveryInterval)
		defer ticker.Stop()

		var pingData [2]byte
		binary.LittleEndian.PutUint16(pingData[:], peer.Port)

		message := wire.Msg{
			Version: wire.MsgVersion1,
			Type:    wire.MsgTypePing,
			Data:    pingData[:],
		}

		peerDiscoveryEvent := Event{
			Type:    DiscoverPeers,
			Message: message,
		}

		peer.Opts.Logger.Debug("peer discovery starting", zap.String("self", peer.Self.String()[:4]))
	LOOP:
		for {
			select {
			case <-ctx.Done():
				peer.Opts.Logger.Debug("peer discovery stopping", zap.String("self", peer.Self.String()[:4]))
				break LOOP

			case <-ticker.C:
				peer.Events <- peerDiscoveryEvent
			}
		}
	}()

	peer.Opts.Logger.Debug("peer event loop starting", zap.String("self", peer.Self.String()[:4]))
	for {
		select {
		case <-ctx.Done():
			peer.Opts.Logger.Debug("peer event loop stopping", zap.String("self", peer.Self.String()[:4]))
			for _, peerConnection := range peer.LinkedPeers {
				close(peerConnection.OutgoingMessages)
				if peerConnection.Connection != nil {
					peerConnection.Connection.Close()
				}
			}
			for _, ephemeralConnection := range peer.EphemeralConnections {
				close(ephemeralConnection.OutgoingMessages)
				if ephemeralConnection.Connection != nil {
					ephemeralConnection.Connection.Close()
				}
			}
			return ctx.Err()

		case event := <-peer.Events:
			peer.handleEvent(event)
		}
	}
}

func (peer *Peer) Link(remote id.Signatory) error {
	responder := make(chan error, 1)
	event := Event{
		Type:           LinkPeer,
		ID:             remote,
		ErrorResponder: responder,
	}

	select {
	case peer.Events <- event:
		return <-responder

	default:
		return ErrEventLoopFull
	}
}

func (peer *Peer) Unlink(remote id.Signatory) error {
	event := Event{
		Type: UnlinkPeer,
		ID:   remote,
	}

	select {
	case peer.Events <- event:
		return nil

	default:
		return ErrEventLoopFull
	}
}

func (peer *Peer) Sync(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	event, errResponder, responder := syncEvent(ctx, contentID, hint)

	select {
	case peer.Events <- event:

	default:
		return nil, ErrEventLoopFull
	}

	return syncResponse(ctx, errResponder, responder)
}

func (peer *Peer) SyncNonBlocking(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	event, errResponder, responder := syncEvent(ctx, contentID, hint)

	select {
	case peer.Events <- event:

	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return syncResponse(ctx, errResponder, responder)
}

func syncEvent(ctx context.Context, contentID []byte, hint *id.Signatory) (Event, chan error, chan []byte) {
	message := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePull,
		Data:    contentID,
	}

	responder := make(chan []byte, 1)
	errResponder := make(chan error, 1)
	event := Event{
		Type:             SyncRequest,
		Message:          message,
		Hint:             hint,
		Ctx:              ctx,
		MessageResponder: responder,
		ErrorResponder:   errResponder,
	}

	return event, errResponder, responder
}

func syncResponse(ctx context.Context, errResponder chan error, responder chan []byte) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case err := <-errResponder:
		return nil, err

	case response := <-responder:
		return response, nil
	}
}

func (peer *Peer) Gossip(ctx context.Context, contentID []byte, subnet *id.Hash) error {
	event := gossipEvent(contentID, subnet)

	select {
	case peer.Events <- event:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (peer *Peer) GossipNonBlocking(contentID []byte, subnet *id.Hash) error {
	event := gossipEvent(contentID, subnet)

	select {
	case peer.Events <- event:
		return nil

	default:
		return ErrEventLoopFull
	}
}

func gossipEvent(contentID []byte, subnet *id.Hash) Event {
	if subnet == nil {
		subnet = &DefaultSubnet
	}

	gossipMessage := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePush,
		To:      *subnet,
		Data:    contentID,
	}

	return Event{
		Type:    GossipMessage,
		Message: gossipMessage,
	}
}

func (peer *Peer) Send(ctx context.Context, data []byte, remote id.Signatory) error {
	event, errResponder := sendEvent(data, remote)

	select {
	case peer.Events <- event:

	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case err := <-errResponder:
		return err
	}
}

func (peer *Peer) SendNonBlocking(data []byte, remote id.Signatory) error {
	event, errResponder := sendEvent(data, remote)

	select {
	case peer.Events <- event:

	default:
		return ErrEventLoopFull
	}

	return <-errResponder
}

func sendEvent(data []byte, remote id.Signatory) (Event, chan error) {
	sendMessage := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypeSend,
		To:      id.Hash(remote),
		Data:    data,
	}

	errResponder := make(chan error, 1)
	return Event{
		Type:           SendMessage,
		ID:             remote,
		Message:        sendMessage,
		ErrorResponder: errResponder,
	}, errResponder
}

func (peer *Peer) listenerHandler(conn net.Conn) {
	peer.Opts.Logger.Debug("incoming connection")

	// TODO(ross): This is a potential avenue for unbounded go routine being
	// spawned. We have ip based rate limiting, but maybe we need to consider
	// protection against attacks involving many ip addresses.
	go func() {
		gcmSession, remote, err := handshake(peer.PrivKey, conn)
		if err != nil {
			peer.Opts.Logger.Warn("handshake failed", zap.Error(err))
			conn.Close()
			return
		}

		newConnectionEvent := Event{
			Type:       NewConnection,
			ID:         remote,
			Connection: conn,
			GCMSession: gcmSession,
		}
		peer.Events <- newConnectionEvent
	}()
}

func (peer *Peer) handleEvent(event Event) {
	peer.Opts.Logger.Debug("handling event", zap.String("self", peer.Self.String()[:4]), zap.String("type", event.Type.String()))
	remote := event.ID

	switch event.Type {
	case IncomingMessage:
		message := event.Message

		switch event.Message.Type {
		case wire.MsgTypePush:
			if len(message.Data) != 0 {
				if _, ok := peer.ContentResolver.QueryContent(message.Data); !ok {
					if peer.hasSpaceForNewGossipSubnet() {
						expiry := time.Now().Add(peer.Opts.GossipTimeout)
						peer.GossipSubnets[string(message.Data)] = GossipSubnet{
							Subnet: message.To,
							Expiry: expiry,
						}

						peer.Filter.allow(message.Data)

						pullMessage := wire.Msg{
							Version: wire.MsgVersion1,
							Type:    wire.MsgTypePull,
							To:      id.Hash(remote),
							Data:    message.Data,
						}

						err := peer.handleSendMessage(remote, pullMessage)
						if err != nil {
							peer.Opts.Logger.Warn("pulling", zap.String("peer", remote.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(message.Data)), zap.Error(err))
						}
					}
				} else {
					// TODO(ross): In this case we certainly don't want to
					// pull, but should we forward on the push anyway? It might
					// be that a peer is regossiping the same message to try to
					// increase network coverage.
				}
			}

		case wire.MsgTypePull:
			if len(message.Data) != 0 {

				content, contentOk := peer.ContentResolver.QueryContent(message.Data)
				if !contentOk {
					peer.Opts.Logger.Debug("missing content", zap.String("peer", remote.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(message.Data)))
				} else {
					syncMessage := wire.Msg{
						Version:  wire.MsgVersion1,
						To:       id.Hash(remote),
						Type:     wire.MsgTypeSync,
						Data:     message.Data,
						SyncData: content,
					}

					err := peer.handleSendMessage(remote, syncMessage)
					if err != nil {
						peer.Opts.Logger.Warn("syncing", zap.String("peer", remote.String()), zap.String("id", base64.RawURLEncoding.EncodeToString(message.Data)), zap.Error(err))
					}
				}
			}

		case wire.MsgTypeSync:
			contentID := string(message.Data)

			if !peer.Filter.filter(remote, message) {
				if pendingSync, ok := peer.PendingSyncs[contentID]; ok {
					for _, responder := range pendingSync.Responders {
						responder <- message.SyncData
					}

					delete(peer.PendingSyncs, contentID)
				}

				if len(message.Data) != 0 && len(message.SyncData) != 0 {
					_, alreadySeenContent := peer.ContentResolver.QueryContent(message.Data)

					if !alreadySeenContent {
						peer.ContentResolver.InsertContent(message.Data, message.SyncData)
					}
				}

				if gossipSubnet, ok := peer.GossipSubnets[contentID]; ok {
					pushMessage := wire.Msg{
						Version: wire.MsgVersion1,
						To:      gossipSubnet.Subnet,
						Type:    wire.MsgTypePush,
						Data:    message.Data,
					}

					peer.gossip(pushMessage)

					delete(peer.GossipSubnets, contentID)
					peer.Filter.deny(message.Data)
				}
			}

		case wire.MsgTypeSend:
			if peer.Receive != nil {
				peer.Receive(event.ID, event.Message.Data)
			}

		case wire.MsgTypePing:
			if dataLen := len(message.Data); dataLen != 2 {
				peer.Opts.Logger.Warn("malformed port", zap.String("peer", remote.String()), zap.Uint64("port byte size", uint64(dataLen)))
			}
			port := binary.LittleEndian.Uint16(message.Data)

			peer.PeerTable.AddPeer(
				remote,
				wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", event.Addr.(*net.TCPAddr).IP.String(), port), uint64(time.Now().UnixNano())),
			)

			peers := peer.PeerTable.RandomPeers(peer.Opts.PongAlpha)
			addrAndSig := make([]wire.SignatoryAndAddress, 0, len(peers))
			for _, sig := range peers {
				addr, addrOk := peer.PeerTable.PeerAddress(sig)
				if !addrOk {
					// NOTE(ross): This is a DPanic because currently the
					// assumption for the table is that it will only contain a
					// peer if it also contains an address for that peer.
					peer.Opts.Logger.DPanic("peer does not exist in table", zap.String("peer", sig.String()))
					continue
				}
				sigAndAddr := wire.SignatoryAndAddress{Signatory: sig, Address: addr}
				addrAndSig = append(addrAndSig, sigAndAddr)
			}

			addrAndSigBytes, err := surge.ToBinary(addrAndSig)
			if err != nil {
				peer.Opts.Logger.DPanic("marshalling error", zap.Error(err))
			}
			response := wire.Msg{
				Version: wire.MsgVersion1,
				Type:    wire.MsgTypePingAck,
				To:      id.Hash(remote),
				Data:    addrAndSigBytes,
			}
			if err := peer.handleSendMessage(remote, response); err != nil {
				peer.Opts.Logger.Warn("failed to send ping ack", zap.String("peer", remote.String()), zap.Error(err))
			}

		case wire.MsgTypePingAck:
			signatoriesAndAddrs := []wire.SignatoryAndAddress{}
			err := surge.FromBinary(&signatoriesAndAddrs, message.Data)
			if err != nil {
				peer.Opts.Logger.Warn("unmarshaling ping ack", zap.String("peer", remote.String()), zap.Error(err))
			}

			for _, signatoryAndAddr := range signatoriesAndAddrs {
				// NOTE(ross): We rely on the fact that the peer table won't
				// add itself.
				peer.PeerTable.AddPeer(signatoryAndAddr.Signatory, signatoryAndAddr.Address)
			}

		default:
			peer.Opts.Logger.Warn("unsupported messge type", zap.Uint16("type", message.Type))
		}

	case SendMessage:
		event.ErrorResponder <- peer.handleSendMessage(remote, event.Message)

	case GossipMessage:
		peer.gossip(event.Message)

	case SyncRequest:
		contentID := event.Message.Data

		if pendingSync, ok := peer.PendingSyncs[string(contentID)]; ok {
			if uint(len(pendingSync.Responders)) >= peer.Opts.MaxActiveSyncsForSameContent {
				event.ErrorResponder <- ErrTooManySyncsForSameContent
			} else {
				pendingSync.Responders = append(pendingSync.Responders, event.MessageResponder)
			}
		} else {
			peer.Filter.allow(contentID)

			if uint(len(peer.PendingSyncs)) >= peer.Opts.MaxPendingSyncs {
				event.ErrorResponder <- ErrTooManyPendingSyncs
			} else {
				pendingSync := PendingSync{
					Ctx:        event.Ctx,
					Responders: []chan<- []byte{event.MessageResponder},
				}

				peer.PendingSyncs[string(contentID)] = pendingSync
			}
		}

		peers := peer.PeerTable.RandomPeers(peer.Opts.GossipAlpha)
		if event.Hint != nil {
			peers = append([]id.Signatory{*event.Hint}, peers...)
		}

		message := event.Message
		warnThreshold := len(peers) / 2
		numErrors := 0
		for _, recipient := range peers {
			if err := peer.handleSendMessage(recipient, message); err != nil {
				if recipient.Equal(event.Hint) {
					peer.Opts.Logger.Warn("unable to sync from hinted peer", zap.String("peer", recipient.String()), zap.Error(err))
				}
				numErrors++
			}
		}

		if numErrors > warnThreshold {
			peer.Opts.Logger.Warn("low sync gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(peers)-numErrors, len(peers))))
		}

	case ReaderDropped:
		peer.Opts.Logger.Debug("reader dropped", zap.String("self", peer.Self.String()[:4]), zap.Error(event.Error))
		// TODO(ross): If the error was malicious we should act accordingly.

		if linkedPeer, ok := peer.LinkedPeers[remote]; ok {
			peer.TearDownConnection(linkedPeer)

			remoteAddr, ok := peer.PeerTable.PeerAddress(event.ID)
			if ok && remoteAddr.Protocol == wire.TCP {
				go dialAndPublishEvent(peer.Ctx, peer.Ctx, peer.Events, peer.PrivKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, event.ID, remoteAddr.Value)
			}
		} else if ephemeralConnection, ok := peer.EphemeralConnections[remote]; ok {
			peer.TearDownConnection(&ephemeralConnection.PeerConnection)
			if ephemeralConnection.PendingMessage != nil {
				peer.Opts.Logger.Warn("ephemeral connection dropped with unsent message", zap.String("peer", remote.String()))
			}
			delete(peer.EphemeralConnections, remote)
		} else {
			// Do nothing.
		}

	case WriterDropped:
		if linkedPeer, ok := peer.LinkedPeers[remote]; ok {
			peer.TearDownConnection(linkedPeer)

			remoteAddr, ok := peer.PeerTable.PeerAddress(event.ID)
			if ok && remoteAddr.Protocol == wire.TCP {
				go dialAndPublishEvent(peer.Ctx, peer.Ctx, peer.Events, peer.PrivKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, event.ID, remoteAddr.Value)
			}
		} else if ephemeralConnection, ok := peer.EphemeralConnections[remote]; ok {
			peer.TearDownConnection(&ephemeralConnection.PeerConnection)
			if ephemeralConnection.PendingMessage != nil {
				peer.Opts.Logger.Warn("ephemeral connection dropped with unsent message", zap.String("peer", remote.String()))
			}
			delete(peer.EphemeralConnections, remote)
		} else {
			// Do nothing.
		}

	case NewConnection:
		var peerConnection *PeerConnection
		var wouldKeepAlive bool
		isLinked := false
		if linkedPeer, ok := peer.LinkedPeers[remote]; ok {
			isLinked = true
			peerConnection = linkedPeer
			wouldKeepAlive = linkedPeer.Connection == nil || time.Now().Sub(linkedPeer.Timestamp) > peer.Opts.MinimumConnectionExpiryAge
		} else if ephemeralConnection, ok := peer.EphemeralConnections[remote]; ok {
			peerConnection = &ephemeralConnection.PeerConnection
			wouldKeepAlive = ephemeralConnection.Connection == nil && time.Now().Before(ephemeralConnection.ExpiryDeadline)
		} else {
			if uint(len(peer.EphemeralConnections)) < peer.Opts.MaxEphemeralConnections {
				expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)

				ephemeralConnection := &EphemeralConnection{
					PeerConnection: PeerConnection{
						Connection:       nil,
						OutgoingMessages: make(chan wire.Msg, peer.Opts.OutgoingBufferSize),
					},
					ExpiryDeadline: expiryDeadline,
				}

				peer.EphemeralConnections[remote] = ephemeralConnection

				peerConnection = &ephemeralConnection.PeerConnection
				wouldKeepAlive = true
			} else {
				peerConnection = nil
				wouldKeepAlive = false
			}
		}

		if peerConnection != nil {
			cmp := bytes.Compare(peer.Self[:], remote[:])
			if cmp == 0 {
				peer.Opts.Logger.DPanic("connection to self", zap.String("self", peer.Self.String()))
			} else if cmp > 0 {
				decisionBuffer := [128]byte{}
				var decisionEncoded []byte
				if wouldKeepAlive {
					peer.TearDownConnection(peerConnection)

					peerConnection.Connection = event.Connection
					peerConnection.GCMSession = event.GCMSession
					peerConnection.Timestamp = time.Now()

					decisionEncoded = encode([]byte{KeepAliveTrue}, decisionBuffer[:], event.GCMSession)

					peer.Opts.Logger.Debug("signalling to keep alive", zap.String("self", peer.Self.String()[:4]), zap.String("remote", remote.String()[:4]))
					// NOTE(ross): Normally whenever we write to a connection
					// we do it in a separate go routine as it could
					// potentially block. However, we don't do this here
					// because theoretically this call should only block if the
					// send buffer in the kernel is full. At this stage, it
					// should be the case that this buffer is empty, and so
					// since the message we are trying to send is very small
					// then for any reasonably configured socket we should not
					// block.
					// NOTE(ross): If later it is decided to move the write
					// into a go routine, make sure to not cause any races
					// (i.e.  don't call peer.StartConnection!)
					_, err := event.Connection.Write(decisionEncoded[:])
					if err != nil {
						peerConnection.Connection.Close()
						peerConnection.Connection = nil

						if isLinked {
							remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
							if ok && remoteAddr.Protocol == wire.TCP {
								go dialAndPublishEvent(peer.Ctx, peer.Ctx, peer.Events, peer.PrivKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, event.ID, remoteAddr.Value)
							}
						}
					} else {
						peer.StartConnection(peerConnection, remote)
					}
				} else {
					// TODO(ross): Should this timeout be configurable?
					event.Connection.SetDeadline(time.Now().Add(5 * time.Second))

					decisionEncoded = encode([]byte{KeepAliveFalse}, decisionBuffer[:], event.GCMSession)

					peer.Opts.Logger.Debug("signalling to drop", zap.String("self", peer.Self.String()[:4]), zap.String("remote", remote.String()[:4]))
					// NOTE(ross): Normally whenever we write to a connection
					// we do it in a separate go routine as it could
					// potentially block. However, we don't do this here
					// because theoretically this call should only block if the
					// send buffer in the kernel is full. At this stage, it
					// should be the case that this buffer is empty, and so
					// since the message we are trying to send is very small
					// then for any reasonably configured socket we should not
					// block.
					event.Connection.Write(decisionEncoded[:])
					event.Connection.Close()
				}
			} else {
				go func() {
					readBuffer := [128]byte{}
					decisionBuffer := [1]byte{}
					decisionDecoded, err := readAndDecode(event.Connection, event.GCMSession, nil, readBuffer[:], decisionBuffer[:])
					if err != nil {
					} else {
						peer.Opts.Logger.Debug("received keep alive message", zap.String("self", peer.Self.String()[:4]), zap.String("remote", remote.String()[:4]), zap.Uint8("decision", decisionDecoded[0]))
					}

					if err == nil && decisionDecoded[0] == KeepAliveTrue {
						peer.Events <- Event{
							Type:       KeepAlive,
							ID:         remote,
							Connection: event.Connection,
							GCMSession: event.GCMSession,
						}
					}
				}()
			}
		}

	case KeepAlive:
		var peerConnection *PeerConnection
		if linkedPeer, ok := peer.LinkedPeers[remote]; ok {
			peerConnection = linkedPeer
		} else if ephemeralConnection, ok := peer.EphemeralConnections[remote]; ok {
			if time.Now().Before(ephemeralConnection.ExpiryDeadline) {
				peerConnection = &ephemeralConnection.PeerConnection
			} else {
				peerConnection = nil
			}
		} else {
			peerConnection = nil
		}

		if peerConnection != nil {
			remote := event.ID
			peer.TearDownConnection(peerConnection)

			peerConnection.Connection = event.Connection
			peerConnection.GCMSession = event.GCMSession
			peerConnection.Timestamp = time.Now()

			peer.StartConnection(peerConnection, remote)
		}

	case DialTimeout:
		remote := event.ID

		peer.PeerTable.AddExpiry(remote, peer.Opts.PeerExpiryTimeout)

		expired := peer.PeerTable.HandleExpired(remote)
		if !expired {
			if _, ok := peer.LinkedPeers[remote]; ok {
				// This can happen if an ephemeral connection that was still dialling
				// was upgraded to a linked peer.

				remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
				if ok && remoteAddr.Protocol == wire.TCP {
					go dialAndPublishEvent(peer.Ctx, peer.Ctx, peer.Events, peer.PrivKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, event.ID, remoteAddr.Value)
				}
			} else if _, ok := peer.EphemeralConnections[remote]; ok {
				delete(peer.EphemeralConnections, remote)
			} else {
				// Do nothing.
			}
		}

	case LinkPeer:
		if _, ok := peer.LinkedPeers[remote]; ok {
			// Do nothing.
			event.ErrorResponder <- nil
		} else if ephemeralConnection, ok := peer.EphemeralConnections[remote]; ok {
			// Upgrade to a linked peer.

			remote := event.ID

			if len(peer.LinkedPeers) >= int(peer.Opts.MaxLinkedPeers) {
				event.ErrorResponder <- ErrTooManyLinkedPeers
			} else {
				peerConnection := ephemeralConnection.PeerConnection
				delete(peer.EphemeralConnections, remote)
				peer.LinkedPeers[remote] = &peerConnection

				event.ErrorResponder <- nil
			}
		} else {
			// Create a new linked peer.
			if len(peer.LinkedPeers) >= int(peer.Opts.MaxLinkedPeers) {
				event.ErrorResponder <- ErrTooManyLinkedPeers
			} else {
				peerConnection := PeerConnection{
					Connection:       nil,
					OutgoingMessages: make(chan wire.Msg, peer.Opts.OutgoingBufferSize),
				}

				remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
				if ok && remoteAddr.Protocol == wire.TCP {
					go dialAndPublishEvent(peer.Ctx, peer.Ctx, peer.Events, peer.PrivKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
				}

				peer.LinkedPeers[remote] = &peerConnection

				event.ErrorResponder <- nil
			}
		}

	case DiscoverPeers:
		recipients := peer.PeerTable.RandomPeers(peer.Opts.PingAlpha)
		warnThreshold := len(recipients) / 2
		numErrors := 0
		for _, recipient := range recipients {
			if err := peer.handleSendMessage(recipient, event.Message); err != nil {
				numErrors++
			}
		}

		if numErrors > warnThreshold {
			peer.Opts.Logger.Warn("low ping gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(recipients)-numErrors, len(recipients))))
		}

	case UnlinkPeer:
		if linkedPeer, ok := peer.LinkedPeers[remote]; ok {
			if linkedPeer.Connection != nil {
				linkedPeer.Connection.Close()
			}

			delete(peer.LinkedPeers, event.ID)
		} else {
			// Do nothing.
		}

	default:
		panic(fmt.Sprintf("unexpected variant: %v", event.Type))
	}
}

func (peer *Peer) gossip(message wire.Msg) {
	subnet := id.Hash(message.To)
	recipients := []id.Signatory{}
	if subnet.Equal(&DefaultSubnet) {
		recipients = peer.PeerTable.Peers(peer.Opts.GossipAlpha)
	} else {
		if recipients = peer.PeerTable.Subnet(subnet); len(recipients) > peer.Opts.GossipAlpha {
			recipients = recipients[:peer.Opts.GossipAlpha]
		}
	}

	warnThreshold := len(recipients) / 2
	numErrors := 0
	for _, recipient := range recipients {
		if err := peer.handleSendMessage(recipient, message); err != nil {
			numErrors++
		}
	}

	if numErrors > warnThreshold {
		peer.Opts.Logger.Warn("low gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(recipients)-numErrors, len(recipients))))
	}
}

func (peer *Peer) hasSpaceForNewGossipSubnet() bool {
	if uint(len(peer.GossipSubnets)) < peer.Opts.MaxGossipSubnets {
		return true
	} else {
		now := time.Now()

		for contentID, gossipSubnet := range peer.GossipSubnets {
			if now.After(gossipSubnet.Expiry) {
				delete(peer.GossipSubnets, contentID)
				peer.Filter.deny([]byte(contentID))
			}
		}

		return uint(len(peer.GossipSubnets)) < peer.Opts.MaxGossipSubnets
	}
}

func (peer *Peer) handleSendMessage(remote id.Signatory, message wire.Msg) error {
	if linkedPeer, ok := peer.LinkedPeers[remote]; ok {
		ctx, cancel := context.WithTimeout(peer.Ctx, peer.Opts.OutgoingBufferTimeout)
		defer cancel()

		select {
		case linkedPeer.OutgoingMessages <- message:
			return nil

		case <-ctx.Done():
			if peer.Ctx.Err() == nil {
				peer.Opts.Logger.Warn("outgoing message buffer back pressure")
				return ErrMessageBufferFull
			} else {
				return nil
			}
		}
	} else if ephemeralConnection, ok := peer.EphemeralConnections[remote]; ok {
		expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)
		ephemeralConnection.ExpiryDeadline = expiryDeadline
		if ephemeralConnection.Connection != nil {
			ephemeralConnection.Connection.SetDeadline(ephemeralConnection.ExpiryDeadline)
		}

		select {
		case ephemeralConnection.OutgoingMessages <- message:
			return nil

		default:
			return ErrMessageBufferFull
		}
	} else {
		if len(peer.EphemeralConnections) >= int(peer.Opts.MaxEphemeralConnections) {
			return ErrTooManyEphemeralConnections
		} else {
			expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)

			ephemeralConnection := &EphemeralConnection{
				PeerConnection: PeerConnection{
					Connection:       nil,
					OutgoingMessages: make(chan wire.Msg, peer.Opts.OutgoingBufferSize),
				},
				ExpiryDeadline: expiryDeadline,
			}

			remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
			if ok && remoteAddr.Protocol == wire.TCP {
				ctx, cancel := context.WithTimeout(peer.Ctx, peer.Opts.EphemeralConnectionTTL)
				go func() {
					dialAndPublishEvent(ctx, peer.Ctx, peer.Events, peer.PrivKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
					cancel()
				}()
			}

			ephemeralConnection.OutgoingMessages <- message

			peer.EphemeralConnections[remote] = ephemeralConnection

			return nil
		}
	}
}

func (peer *Peer) TearDownConnection(peerConnection *PeerConnection) {
	peer.Opts.Logger.Debug("tearing down connection", zap.String("self", peer.Self.String()[:4]))
	if peerConnection.Connection != nil {
		peerConnection.Connection.Close()
		peerConnection.Connection = nil

		// We create a new channel so that we can signal to the writer to
		// finish if it is blocking on reading from the outgoing message
		// channel.
		newOutgoingBuffer := make(chan wire.Msg, peer.Opts.OutgoingBufferSize)
	LOOP:
		for {
			select {
			case msg := <-peerConnection.OutgoingMessages:
				newOutgoingBuffer <- msg

			default:
				break LOOP
			}
		}
		close(peerConnection.OutgoingMessages)
		peerConnection.OutgoingMessages = newOutgoingBuffer

		// NOTE(ross): Waiting for the reading and writing go routines to finish
		// relies on the fact that closing the connection will cause blocked
		// reads/writes to return, otherwise this will block the whole event loop.
		// This behaviour is sepcified by the documentation for `Close`, so we
		// should be OK.
		<-peerConnection.ReadDone
		peerConnection.PendingMessage = <-peerConnection.WriteDone
	}

}

func (peer *Peer) StartConnection(peerConnection *PeerConnection, remote id.Signatory) {
	peer.Opts.Logger.Debug("starting connection", zap.String("self", peer.Self.String()[:4]))
	peerConnection.ReadDone = make(chan struct{}, 1)
	peerConnection.WriteDone = make(chan *wire.Msg, 1)

	firstMessage := peerConnection.PendingMessage
	peerConnection.PendingMessage = nil

	go read(
		peer.Ctx,
		peerConnection.Connection,
		peerConnection.GCMSession,
		peer.Filter,
		peer.Events,
		remote,
		peer.Opts.ConnectionRateLimiterOptions,
		peer.Opts.MaxMessageSize,
		peerConnection.ReadDone,
	)
	go write(
		peer.Ctx,
		peer.Opts.WriteTimeout,
		peerConnection.Connection,
		peerConnection.GCMSession,
		peer.Events,
		peerConnection.OutgoingMessages,
		peer.Opts.MaxMessageSize,
		peerConnection.WriteDone,
		remote,
		firstMessage,
	)
}

func dialAndPublishEvent(
	ctx, peerCtx context.Context,
	events chan Event,
	privKey *id.PrivKey,
	logger *zap.Logger,
	dialRetryInterval time.Duration,
	remote id.Signatory,
	remoteAddr string,
) {
	conn, err := dial(ctx, remoteAddr, dialRetryInterval, logger)

	var event Event
	event.ID = remote
	if err != nil {
		event.Type = DialTimeout
		event.Error = err
	} else {
		gcmSession, discoveredRemote, err := handshake(privKey, conn)

		if err != nil {
			logger.Warn("handshake failed", zap.Error(err))
			conn.Close()

			event.Type = DialTimeout
			event.Error = err
		} else if !remote.Equal(&discoveredRemote) {
			// TODO(ross): What to do here? This being an error probably relies
			// on only using signed addresses during peer discovery.
		} else {
			event.Type = NewConnection
			event.Connection = conn
			event.GCMSession = gcmSession
		}
	}

	select {
	case <-peerCtx.Done():
	case events <- event:
	}
}

func read(
	ctx context.Context,
	conn net.Conn,
	gcmSession *session.GCMSession,
	filter *syncFilter,
	events chan<- Event,
	remote id.Signatory,
	rateLimiterOptions RateLimiterOptions,
	maxMessageSize uint,
	done chan<- struct{},
) {
	unmarshalBuffer := make([]byte, maxMessageSize)
	decodeBuffer := make([]byte, maxMessageSize)

	rateLimiter := rate.NewLimiter(rateLimiterOptions.Rate, rateLimiterOptions.Burst)

	addr := conn.RemoteAddr()

	for {
		decodedMessage, err := readAndDecode(conn, gcmSession, rateLimiter, decodeBuffer, unmarshalBuffer)
		if err != nil {
			event := Event{
				Type:  ReaderDropped,
				ID:    remote,
				Error: err,
			}

			select {
			case <-ctx.Done():
			case events <- event:
			}

			close(done)
			return
		}

		msg := wire.Msg{}
		_, _, err = msg.Unmarshal(decodedMessage, len(decodedMessage))
		if err != nil {
			event := Event{
				Type:  ReaderDropped,
				ID:    remote,
				Error: fmt.Errorf("unmarshalling message: %v", err),
			}

			select {
			case <-ctx.Done():
			case events <- event:
			}

			close(done)
			return
		} else {
			if msg.Type == wire.MsgTypeSync {
				if filter.filter(remote, msg) {
					event := Event{
						Type:  ReaderDropped,
						ID:    remote,
						Error: fmt.Errorf("unexpected sync for content %v", base64.RawURLEncoding.EncodeToString(msg.Data)),
					}

					select {
					case <-ctx.Done():
					case events <- event:
					}

					close(done)
					return
				}

				decodedSyncData, err := readAndDecode(conn, gcmSession, rateLimiter, decodeBuffer, unmarshalBuffer)
				if err != nil {
					event := Event{
						Type:  ReaderDropped,
						ID:    remote,
						Error: err,
					}

					select {
					case <-ctx.Done():
					case events <- event:
					}

					close(done)
					return
				}

				msg.SyncData = make([]byte, len(decodedSyncData))
				copy(msg.SyncData, decodedSyncData)
			}

			event := Event{
				Type:    IncomingMessage,
				ID:      remote,
				Message: msg,
				Addr:    addr,
			}

			select {
			case <-ctx.Done():
				return
			case events <- event:
			}
		}
	}
}

func readAndDecode(conn net.Conn, gcmSession *session.GCMSession, rateLimiter *rate.Limiter, readBuffer, decodeBuffer []byte) ([]byte, error) {
	lengthBuffer := readBuffer[:4]
	_, err := io.ReadFull(conn, lengthBuffer)
	if err != nil {
		return nil, fmt.Errorf("reading length prefix: %v", err)
	}

	length := binary.BigEndian.Uint32(lengthBuffer)
	if uint32(cap(readBuffer)-4) < length {
		return nil, fmt.Errorf("message length %v exceeds buffer size %v", length, cap(readBuffer)-4)
	}

	// NOTE(ross): Casting to int here could technically overflow, but for
	// that to happen `length` would have to be so big that it surely fails
	// the previous check.
	if rateLimiter != nil && !rateLimiter.AllowN(time.Now(), int(4+length)) {
		return nil, errors.New("rate limit exceeded")
	}

	encryptedMessageBuffer := readBuffer[4 : 4+length]
	_, err = io.ReadFull(conn, encryptedMessageBuffer)
	if err != nil {
		return nil, fmt.Errorf("reading message body: %v", err)
	}

	decodedMessage, err := decode(encryptedMessageBuffer, decodeBuffer, gcmSession)
	if err != nil {
		return nil, fmt.Errorf("decoding message: %v", err)
	}

	return decodedMessage, nil
}

func write(
	ctx context.Context,
	writeTimeout time.Duration,
	conn net.Conn,
	gcmSession *session.GCMSession,
	events chan<- Event,
	outgoingMessages chan wire.Msg,
	maxMessageSize uint,
	done chan<- *wire.Msg,
	remote id.Signatory,
	firstMessage *wire.Msg,
) {
	marshalBuffer := make([]byte, maxMessageSize)
	encodeBuffer := make([]byte, maxMessageSize)

	var msg wire.Msg
	var ok bool
	for {
		if firstMessage != nil {
			msg, ok = *firstMessage, true
			firstMessage = nil
		} else {
			select {
			case <-ctx.Done():
				return
			case msg, ok = <-outgoingMessages:
			}
		}

		if !ok {
			close(done)
			return
		} else {
			marshalBuffer = marshalBuffer[:cap(marshalBuffer)]
			encodeBuffer = encodeBuffer[:cap(encodeBuffer)]

			tail, _, err := msg.Marshal(marshalBuffer, len(marshalBuffer))
			if err != nil {
				panic("marshalling outgoing message")
			}
			marshalBuffer = marshalBuffer[:len(marshalBuffer)-len(tail)]

			encodeBuffer = encode(marshalBuffer, encodeBuffer, gcmSession)

			// We set a write deadline here to account for malicious or
			// otherwise peers that cause writing to block indefinitely. In any
			// case, the right course of action for such blocking is probably
			// dropping the peer.
			conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(writeTimeout))

			n, err := conn.Write(encodeBuffer)

			if err != nil || n != len(encodeBuffer) {
				done <- &msg
				event := Event{
					Type:  WriterDropped,
					ID:    remote,
					Error: err,
				}

				select {
				case events <- event:
				case <-ctx.Done():
				}

				close(done)
				return
			}

			if msg.Type == wire.MsgTypeSync {
				encodeBuffer = encode(msg.SyncData, encodeBuffer, gcmSession)

				n, err := conn.Write(encodeBuffer)

				if err != nil || n != len(encodeBuffer) {
					done <- &msg
					event := Event{
						Type:  WriterDropped,
						ID:    remote,
						Error: err,
					}

					select {
					case events <- event:
					case <-ctx.Done():
					}

					close(done)
					return
				}
			}
		}
	}
}
