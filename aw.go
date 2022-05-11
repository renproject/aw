package aw

/* TODO(ross): Features
 *    - Use signed addresses for peer discovery security
 */

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
	"github.com/renproject/aw/handshake"
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
	DefaultMaxGossipSubnets             uint          = 100
	DefaultMaxMessageSize               uint          = 4 * 1024 * 1024
	DefaultOutgoingBufferSize           uint          = 100
	DefaultIncomingBufferSize           uint          = 100
	DefaultEventLoopBufferSize          uint          = 100
	DefaultOutgoingBufferTimeout        time.Duration = time.Second
	DefaultWriteTimeout                 time.Duration = time.Second
	DefaultDialRetryInterval            time.Duration = time.Second
	DefaultEphemeralConnectionTTL       time.Duration = 10 * time.Second
	DefaultMinimumConnectionExpiryAge   time.Duration = time.Minute
	DefaultGossipAlpha                  int           = 5
	DefaultGossipTimeout                time.Duration = 5 * time.Second
	DefaultPingAlpha                    int           = 5
	DefaultPongAlpha                    int           = 5
	DefaultPeerDiscoveryInterval        time.Duration = 30 * time.Second
	DefaultPeerExpiryTimeout            time.Duration = time.Minute
)

var (
	ErrTooManyLinkedPeers          = errors.New("too many linked peers")
	ErrTooManyEphemeralConnections = errors.New("too many ephemeral connections")
	ErrMessageBufferFull           = errors.New("outgoing message buffer is full")
	ErrEventLoopFull               = errors.New("event loop buffer is full")
	ErrTooManyPendingSyncs         = errors.New("too many pending sync requests")
	ErrTooManySyncsForSameContent  = errors.New("too many simultaneous syncs for the same content ID")
)

type eventType uint

const (
	incomingMessage eventType = iota
	sendMessage
	gossipMessage
	syncRequest
	readerDropped
	writerDropped
	newConnection
	keepAlive
	dialTimeout
	linkPeer
	discoverPeers
	unlinkPeer
)

const (
	keepAliveFalse byte = 0x00
	keepAliveTrue  byte = 0x01
)

func (ty eventType) String() string {
	switch ty {
	case incomingMessage:
		return "IncomingMessage"
	case sendMessage:
		return "SendMessage"
	case gossipMessage:
		return "GossipMessage"
	case syncRequest:
		return "SyncRequest"
	case readerDropped:
		return "ReaderDropped"
	case writerDropped:
		return "WriterDropped"
	case newConnection:
		return "NewConnection"
	case keepAlive:
		return "KeepAlive"
	case dialTimeout:
		return "DialTimeout"
	case linkPeer:
		return "LinkPeer"
	case discoverPeers:
		return "DiscoverPeers"
	case unlinkPeer:
		return "UnlinkPeer"
	default:
		return fmt.Sprintf("unknown(%v)", uint(ty))
	}
}

type event struct {
	ty eventType

	id         id.Signatory
	subnet     id.Hash
	hint       *id.Signatory
	message    wire.Msg
	addr       net.Addr
	connection net.Conn
	gcmSession *session.GCMSession
	ctx        context.Context
	err        error

	messageResponder chan<- []byte
	errorResponder   chan<- error
}

type peerConnection struct {
	readDone  chan struct{}
	writeDone chan *wire.Msg

	connection       net.Conn
	gcmSession       *session.GCMSession
	timestamp        time.Time
	outgoingMessages chan wire.Msg
	pendingMessage   *wire.Msg
}

type ephemeralConnection struct {
	peerConnection

	expiryDeadline time.Time
}

type pendingSync struct {
	ctx        context.Context
	responders []chan<- []byte
}

type gossipSubnet struct {
	subnet id.Hash
	expiry time.Time
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
	IncomingBufferSize           uint
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
		IncomingBufferSize:           DefaultIncomingBufferSize,
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

type IncomingMessage struct {
	From id.Signatory
	Data []byte
}

type Peer struct {
	Opts Options

	id      id.Signatory
	addr    wire.Address
	privKey *id.PrivKey
	Port    uint16

	ctx                  context.Context
	events               chan event
	linkedPeers          map[id.Signatory]*peerConnection
	ephemeralConnections map[id.Signatory]*ephemeralConnection
	pendingSyncs         map[string]pendingSync
	gossipSubnets        map[string]gossipSubnet
	filter               *syncFilter

	PeerTable        dht.Table
	ContentResolver  dht.ContentResolver
	IncomingMessages chan IncomingMessage
}

func New(opts Options, privKey *id.PrivKey, peerTable dht.Table, contentResolver dht.ContentResolver) *Peer {
	self := privKey.Signatory()
	addr := wire.Address{} // TODO(ross): Receive in the contructor or set afterwards?

	events := make(chan event, opts.EventLoopBufferSize)
	linkedPeers := make(map[id.Signatory]*peerConnection, opts.MaxLinkedPeers)
	ephemeralConnections := make(map[id.Signatory]*ephemeralConnection, opts.MaxEphemeralConnections)
	pendingSyncs := make(map[string]pendingSync, opts.MaxPendingSyncs)
	gossipSubnets := make(map[string]gossipSubnet, opts.MaxGossipSubnets)

	filter := newSyncFilter()

	incomingMessages := make(chan IncomingMessage, opts.IncomingBufferSize)

	return &Peer{
		Opts: opts,

		id:      self,
		addr:    addr,
		privKey: privKey,
		Port:    0,

		ctx:                  nil,
		events:               events,
		linkedPeers:          linkedPeers,
		ephemeralConnections: ephemeralConnections,
		pendingSyncs:         pendingSyncs,
		gossipSubnets:        gossipSubnets,
		filter:               filter,

		PeerTable:        peerTable,
		ContentResolver:  contentResolver,
		IncomingMessages: incomingMessages,
	}
}

func (peer *Peer) ID() id.Signatory {
	return peer.privKey.Signatory()
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
	peer.ctx = ctx

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

		peerDiscoveryEvent := event{
			ty:      discoverPeers,
			message: message,
		}

		peer.Opts.Logger.Debug("peer discovery starting", zap.String("self", peer.id.String()[:4]))
	LOOP:
		for {
			select {
			case <-ctx.Done():
				peer.Opts.Logger.Debug("peer discovery stopping", zap.String("self", peer.id.String()[:4]))
				break LOOP

			case <-ticker.C:
				peer.Opts.Logger.Debug("peer discovery event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
				peer.events <- peerDiscoveryEvent
			}
		}
	}()

	peer.Opts.Logger.Debug("peer event loop starting", zap.String("self", peer.id.String()[:4]))
	for {
		select {
		case <-ctx.Done():
			peer.Opts.Logger.Debug("peer event loop stopping", zap.String("self", peer.id.String()[:4]))
			for _, peerConnection := range peer.linkedPeers {
				close(peerConnection.outgoingMessages)
				if peerConnection.connection != nil {
					peerConnection.connection.Close()
				}
			}
			for _, ephemeralConnection := range peer.ephemeralConnections {
				close(ephemeralConnection.outgoingMessages)
				if ephemeralConnection.connection != nil {
					ephemeralConnection.connection.Close()
				}
			}
			return ctx.Err()

		case event := <-peer.events:
			peer.handleEvent(event)
		}
	}
}

func (peer *Peer) Link(remote id.Signatory) error {
	responder := make(chan error, 1)
	event := event{
		ty:             linkPeer,
		id:             remote,
		errorResponder: responder,
	}

	peer.Opts.Logger.Debug("link event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:
		return <-responder

	default:
		return ErrEventLoopFull
	}
}

func (peer *Peer) Unlink(remote id.Signatory) error {
	event := event{
		ty: unlinkPeer,
		id: remote,
	}

	peer.Opts.Logger.Debug("unlink event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:
		return nil

	default:
		return ErrEventLoopFull
	}
}

func (peer *Peer) Sync(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	event, errResponder, responder := syncEvent(ctx, contentID, hint)

	peer.Opts.Logger.Debug("sync event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:

	default:
		return nil, ErrEventLoopFull
	}

	return syncResponse(ctx, errResponder, responder)
}

func (peer *Peer) SyncNonBlocking(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	event, errResponder, responder := syncEvent(ctx, contentID, hint)

	peer.Opts.Logger.Debug("sync nb event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:

	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return syncResponse(ctx, errResponder, responder)
}

func syncEvent(ctx context.Context, contentID []byte, hint *id.Signatory) (event, chan error, chan []byte) {
	message := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePull,
		Data:    contentID,
	}

	responder := make(chan []byte, 1)
	errResponder := make(chan error, 1)
	event := event{
		ty:               syncRequest,
		message:          message,
		hint:             hint,
		ctx:              ctx,
		messageResponder: responder,
		errorResponder:   errResponder,
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

	peer.Opts.Logger.Debug("gossip event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (peer *Peer) GossipNonBlocking(contentID []byte, subnet *id.Hash) error {
	event := gossipEvent(contentID, subnet)

	peer.Opts.Logger.Debug("gossip nb event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:
		return nil

	default:
		return ErrEventLoopFull
	}
}

func gossipEvent(contentID []byte, subnet *id.Hash) event {
	if subnet == nil {
		subnet = &DefaultSubnet
	}

	msg := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypePush,
		To:      *subnet,
		Data:    contentID,
	}

	return event{
		ty:      gossipMessage,
		message: msg,
	}
}

func (peer *Peer) Send(ctx context.Context, data []byte, remote id.Signatory) error {
	event, errResponder := sendEvent(data, remote)

	peer.Opts.Logger.Debug("send event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:

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

	peer.Opts.Logger.Debug("send nb event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
	select {
	case peer.events <- event:

	default:
		return ErrEventLoopFull
	}

	return <-errResponder
}

func sendEvent(data []byte, remote id.Signatory) (event, chan error) {
	msg := wire.Msg{
		Version: wire.MsgVersion1,
		Type:    wire.MsgTypeSend,
		To:      id.Hash(remote),
		Data:    data,
	}

	errResponder := make(chan error, 1)
	return event{
		ty:             sendMessage,
		id:             remote,
		message:        msg,
		errorResponder: errResponder,
	}, errResponder
}

func (peer *Peer) listenerHandler(conn net.Conn) {
	peer.Opts.Logger.Debug("incoming connection")

	// TODO(ross): This is a potential avenue for unbounded go routine being
	// spawned. We have ip based rate limiting, but maybe we need to consider
	// protection against attacks involving many ip addresses.
	go func() {
		gcmSession, remote, err := handshake.Handshake(peer.privKey, conn)
		if err != nil {
			peer.Opts.Logger.Warn("handshake failed", zap.Error(err))
			conn.Close()
			return
		}

		newConnectionEvent := event{
			ty:         newConnection,
			id:         remote,
			connection: conn,
			gcmSession: gcmSession,
		}
		peer.Opts.Logger.Debug("incoming connection event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
		peer.events <- newConnectionEvent
	}()
}

func (peer *Peer) handleEvent(e event) {
	peer.Opts.Logger.Debug("handling event")
	defer peer.Opts.Logger.Debug("done handling event")

	remote := e.id

	switch e.ty {
	case incomingMessage:
		peer.Opts.Logger.Debug("incoming message", zap.String("remote", remote.String()))

		message := e.message

		switch e.message.Type {
		case wire.MsgTypePush:
			peer.Opts.Logger.Debug("push", zap.String("content id", base64.RawURLEncoding.EncodeToString(message.Data)), zap.String("subnet", message.To.String()))

			if len(message.Data) != 0 {
				if _, ok := peer.ContentResolver.QueryContent(message.Data); !ok {
					peer.Opts.Logger.Debug("don't have content")

					if peer.hasSpaceForNewGossipSubnet() {
						expiry := time.Now().Add(peer.Opts.GossipTimeout)
						peer.gossipSubnets[string(message.Data)] = gossipSubnet{
							subnet: message.To,
							expiry: expiry,
						}

						peer.filter.allow(message.Data)

						pullMessage := wire.Msg{
							Version: wire.MsgVersion1,
							Type:    wire.MsgTypePull,
							To:      id.Hash(remote),
							Data:    message.Data,
						}

						err := peer.handleSendMessage(remote, pullMessage)
						if err != nil {
							peer.Opts.Logger.Warn("sending pull", zap.String("peer", remote.String()), zap.Error(err))
						}
					} else {
						peer.Opts.Logger.Debug("don't have space for subnet, ignoring")
					}
				} else {
					// TODO(ross): In this case we certainly don't want to
					// pull, but should we forward on the push anyway? It might
					// be that a peer is regossiping the same message to try to
					// increase network coverage.
					peer.Opts.Logger.Debug("already have content")
				}
			} else {
				peer.Opts.Logger.Warn("empty content id", zap.String("remote", remote.String()))
			}

		case wire.MsgTypePull:
			peer.Opts.Logger.Debug("pull", zap.String("content id", base64.RawURLEncoding.EncodeToString(message.Data)))

			if len(message.Data) != 0 {

				content, contentOk := peer.ContentResolver.QueryContent(message.Data)
				if !contentOk {
					peer.Opts.Logger.Debug("missing content")
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
			} else {
				peer.Opts.Logger.Warn("empty content id", zap.String("remote", remote.String()))
			}

		case wire.MsgTypeSync:
			peer.Opts.Logger.Debug("sync", zap.String("content id", base64.RawURLEncoding.EncodeToString(message.Data)))

			contentID := string(message.Data)

			if !peer.filter.filter(remote, message) {
				var content []byte
				if len(message.Data) != 0 && len(message.SyncData) != 0 {
					oldContent, alreadySeenContent := peer.ContentResolver.QueryContent(message.Data)

					if alreadySeenContent {
						content = oldContent
					} else {
						peer.ContentResolver.InsertContent(message.Data, message.SyncData)

						// NOTE(ross): When inserting content, the content may
						// be deemed to be invalid depending on the content
						// resolver implementation. It is therefore up to the
						// caller of `Sync` to make sure that the returned
						// content is valid in the context of their program.
						content = message.SyncData
					}
				}

				if pendingSync, ok := peer.pendingSyncs[contentID]; ok {
					for _, responder := range pendingSync.responders {
						responder <- content
					}

					delete(peer.pendingSyncs, contentID)
				}

				if gossipSubnet, ok := peer.gossipSubnets[contentID]; ok {
					pushMessage := wire.Msg{
						Version: wire.MsgVersion1,
						To:      gossipSubnet.subnet,
						Type:    wire.MsgTypePush,
						Data:    message.Data,
					}

					peer.gossip(pushMessage)

					delete(peer.gossipSubnets, contentID)
					peer.filter.deny(message.Data)
				}
			} else {
				peer.Opts.Logger.Warn(
					"received sync for unexpected content",
					zap.String("remote", remote.String()),
					zap.String("content id", base64.RawURLEncoding.EncodeToString(message.Data)),
				)
			}

		case wire.MsgTypeSend:
			peer.Opts.Logger.Debug("send")

			peer.Opts.Logger.Debug("writing to incoming messages channel", zap.Int("len", len(peer.IncomingMessages)), zap.Int("cap", cap(peer.IncomingMessages)))
			peer.IncomingMessages <- IncomingMessage{From: remote, Data: e.message.Data}

		case wire.MsgTypePing:
			peer.Opts.Logger.Debug("ping")

			var pingerAddr wire.Address
			err := surge.FromBinary(pingerAddr, message.Data)
			if err != nil {
				peer.Opts.Logger.Warn("malformed address from pinger", zap.String("peer", remote.String()), zap.Error(err))
			}

			// Undefined protocol is used when the peer does not know their own
			// address.
			if pingerAddr.Protocol != wire.UndefinedProtocol {
				peer.PeerTable.AddPeer(remote, pingerAddr)
			}

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

				peer.Opts.Logger.Debug("adding peer to response", zap.String("peer", sig.String()), zap.String("address", addr.String()))
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
				peer.Opts.Logger.Warn("failed to send ping ack", zap.String("remote", remote.String()), zap.Error(err))
			}

		case wire.MsgTypePingAck:
			peer.Opts.Logger.Debug("ping ack")

			signatoriesAndAddrs := []wire.SignatoryAndAddress{}
			err := surge.FromBinary(&signatoriesAndAddrs, message.Data)
			if err != nil {
				peer.Opts.Logger.Warn("unmarshaling ping ack", zap.String("remote", remote.String()), zap.Error(err))
			} else {
				for _, signatoryAndAddr := range signatoriesAndAddrs {
					// NOTE(ross): We rely on the fact that the peer table won't
					// add itself.
					peer.Opts.Logger.Debug("adding peer to table", zap.String("peer", signatoryAndAddr.Signatory.String()), zap.String("address", signatoryAndAddr.Address.String()))
					peer.PeerTable.AddPeer(signatoryAndAddr.Signatory, signatoryAndAddr.Address)
				}
			}

		default:
			peer.Opts.Logger.Warn("unsupported messge type", zap.String("remote", remote.String()), zap.Uint16("type", message.Type))
		}

	case sendMessage:
		peer.Opts.Logger.Debug("send message", zap.String("remote", remote.String()))

		e.errorResponder <- peer.handleSendMessage(remote, e.message)

	case gossipMessage:
		peer.Opts.Logger.Debug("gossip message")

		peer.gossip(e.message)

	case syncRequest:
		peer.Opts.Logger.Debug("sync request", zap.String("remote", remote.String()), zap.String("content id", base64.RawURLEncoding.EncodeToString(e.message.Data)))

		contentID := e.message.Data

		if content, ok := peer.ContentResolver.QueryContent(contentID); ok {
			e.messageResponder <- content
		} else {
			if pending, ok := peer.pendingSyncs[string(contentID)]; ok {
				peer.Opts.Logger.Debug("sync for content id exists")
				if uint(len(pending.responders)) >= peer.Opts.MaxActiveSyncsForSameContent {
					e.errorResponder <- ErrTooManySyncsForSameContent
				} else {
					pending.responders = append(pending.responders, e.messageResponder)

					peer.Opts.Logger.Debug("added pending responder for content id sync", zap.Int("current pending responders", len(pending.responders)))
				}
			} else {
				peer.Opts.Logger.Debug("sync for content id does not exist")

				peer.filter.allow(contentID)

				if numPendingSyncs(peer.pendingSyncs) >= peer.Opts.MaxPendingSyncs {
					e.errorResponder <- ErrTooManyPendingSyncs
				} else {
					pending := pendingSync{
						ctx:        e.ctx,
						responders: []chan<- []byte{e.messageResponder},
					}

					peer.pendingSyncs[string(contentID)] = pending

					peer.Opts.Logger.Debug("added pending sync for content id", zap.Int("current pending syncs", len(peer.pendingSyncs)))
				}
			}

			peers := peer.PeerTable.RandomPeers(peer.Opts.GossipAlpha)
			if e.hint != nil {
				peers = append([]id.Signatory{*e.hint}, peers...)
			}

			message := e.message
			warnThreshold := len(peers) / 2
			numErrors := 0
			for _, recipient := range peers {
				if err := peer.handleSendMessage(recipient, message); err != nil {
					if e.hint != nil && recipient.Equal(e.hint) {
						peer.Opts.Logger.Warn("unable to sync from hinted peer", zap.Error(err))
					}
					numErrors++
				}
			}

			if numErrors > warnThreshold {
				peer.Opts.Logger.Warn("low sync gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(peers)-numErrors, len(peers))))
			}
		}

	case readerDropped:
		peer.Opts.Logger.Debug("reader dropped", zap.String("remote", remote.String()), zap.Error(e.err))

		// TODO(ross): If the error was malicious we should act accordingly.

		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peer.Opts.Logger.Debug("reader dropped for linked peer", zap.Error(e.err))
			peer.tearDownConnection(linkedPeer, remote)

			remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
			if ok && remoteAddr.Protocol == wire.TCP {
				go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
			}
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			peer.Opts.Logger.Debug("reader dropped for ephemeral peer peer", zap.Error(e.err))
			peer.tearDownConnection(&ephemeralConnection.peerConnection, remote)
			if ephemeralConnection.pendingMessage != nil {
				peer.Opts.Logger.Warn("ephemeral connection dropped with unsent message")
			}
			delete(peer.ephemeralConnections, remote)
		} else {
			// Do nothing.
			peer.Opts.Logger.Debug("reader dropped for unlinked peer", zap.Error(e.err))
		}

	case writerDropped:
		peer.Opts.Logger.Debug("writer dropped", zap.String("remote", remote.String()), zap.Error(e.err))

		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peer.Opts.Logger.Debug("writer dropped for linked peer", zap.Error(e.err))
			peer.tearDownConnection(linkedPeer, remote)

			remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
			if ok && remoteAddr.Protocol == wire.TCP {
				go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
			}
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			peer.Opts.Logger.Debug("writer dropped for ephemeral peer peer", zap.Error(e.err))
			peer.tearDownConnection(&ephemeralConnection.peerConnection, remote)
			if ephemeralConnection.pendingMessage != nil {
				peer.Opts.Logger.Warn("ephemeral connection dropped with unsent message")
			}
			delete(peer.ephemeralConnections, remote)
		} else {
			// Do nothing.
			peer.Opts.Logger.Debug("writer dropped for unlinked peer", zap.Error(e.err))
		}

	case newConnection:
		peer.Opts.Logger.Debug("new connection", zap.String("remote", remote.String()))

		var peerConn *peerConnection
		var wouldKeepAlive bool
		isLinked := false
		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			isLinked = true
			peerConn = linkedPeer
			wouldKeepAlive = linkedPeer.connection == nil || time.Since(linkedPeer.timestamp) > peer.Opts.MinimumConnectionExpiryAge

			peer.Opts.Logger.Debug(
				"new connection for linked peer",
				zap.Bool("would keep alive", wouldKeepAlive),
				zap.Bool("don't already have connection", linkedPeer.connection == nil),
				zap.Duration("time since last connection", time.Since(linkedPeer.timestamp)),
			)
		} else if eConn, ok := peer.ephemeralConnections[remote]; ok {
			peerConn = &eConn.peerConnection
			wouldKeepAlive = eConn.connection == nil && time.Now().Before(eConn.expiryDeadline)

			peer.Opts.Logger.Debug(
				"new connection for ephemeral peer",
				zap.Bool("would keep alive", wouldKeepAlive),
				zap.Bool("don't already have connection", eConn.connection == nil),
				zap.Time("old connection expiry deadline", eConn.expiryDeadline),
			)
		} else {
			if uint(len(peer.ephemeralConnections)) < peer.Opts.MaxEphemeralConnections {
				peer.Opts.Logger.Debug("new connection for unlinked peer, creating ephemeral")
				expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)

				eConn := &ephemeralConnection{
					peerConnection: peerConnection{
						connection:       nil,
						outgoingMessages: make(chan wire.Msg, peer.Opts.OutgoingBufferSize),
					},
					expiryDeadline: expiryDeadline,
				}

				peer.ephemeralConnections[remote] = eConn

				peerConn = &eConn.peerConnection
				wouldKeepAlive = true
			} else {
				peer.Opts.Logger.Debug("dropping new connection for unlinked peer, too many ephemeral connections")
				peerConn = nil
				wouldKeepAlive = false
			}
		}

		if peerConn != nil {
			cmp := bytes.Compare(peer.id[:], remote[:])
			if cmp == 0 {
				peer.Opts.Logger.DPanic("connection to self")
			} else if cmp > 0 {
				decisionBuffer := [128]byte{}
				var decisionEncoded []byte
				if wouldKeepAlive {
					peer.Opts.Logger.Debug("signalling to keep alive")

					peer.tearDownConnection(peerConn, remote)

					peerConn.connection = e.connection
					peerConn.gcmSession = e.gcmSession
					peerConn.timestamp = time.Now()

					decisionEncoded = encode([]byte{keepAliveTrue}, decisionBuffer[:], e.gcmSession)
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
					// (i.e. don't call peer.StartConnection!)
					_, err := e.connection.Write(decisionEncoded[:])
					if err != nil {
						peer.Opts.Logger.Debug("failed to send keep alive message")
						peerConn.connection.Close()
						peerConn.connection = nil

						if isLinked {
							remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
							if ok && remoteAddr.Protocol == wire.TCP {
								go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
							}
						}
					} else {
						peer.startConnection(peerConn, remote)
					}
				} else {
					peer.Opts.Logger.Debug("signalling to drop")

					// TODO(ross): Should this timeout be configurable?
					e.connection.SetDeadline(time.Now().Add(5 * time.Second))

					decisionEncoded = encode([]byte{keepAliveFalse}, decisionBuffer[:], e.gcmSession)

					// NOTE(ross): Normally whenever we write to a connection
					// we do it in a separate go routine as it could
					// potentially block. However, we don't do this here
					// because theoretically this call should only block if the
					// send buffer in the kernel is full. At this stage, it
					// should be the case that this buffer is empty, and so
					// since the message we are trying to send is very small
					// then for any reasonably configured socket we should not
					// block.
					e.connection.Write(decisionEncoded[:])
					e.connection.Close()
				}
			} else {
				go func() {
					peer.Opts.Logger.Debug("starting keep alive signal receiver task", zap.String("remote", remote.String()))

					readBuffer := [128]byte{}
					decisionBuffer := [1]byte{}
					decisionDecoded, err := readAndDecode(e.connection, e.gcmSession, nil, readBuffer[:], decisionBuffer[:])
					if err != nil {
						peer.Opts.Logger.Debug("error reading/decoding keep alive message")
					} else {
						peer.Opts.Logger.Debug("received keep alive message", zap.Uint8("decision", decisionDecoded[0]))
					}

					if err == nil && decisionDecoded[0] == keepAliveTrue {
						peer.Opts.Logger.Debug("keep alive event", zap.Int("len", len(peer.events)), zap.Int("cap", cap(peer.events)))
						peer.events <- event{
							ty:         keepAlive,
							id:         remote,
							connection: e.connection,
							gcmSession: e.gcmSession,
						}
					}
				}()
			}
		}

	case keepAlive:
		peer.Opts.Logger.Debug("keep alive", zap.String("remote", remote.String()))

		var peerConn *peerConnection
		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peer.Opts.Logger.Debug("replacing linked peer connection")
			peerConn = linkedPeer
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			if time.Now().Before(ephemeralConnection.expiryDeadline) {
				peer.Opts.Logger.Debug("replacing ephemeral peer connection")
				peerConn = &ephemeralConnection.peerConnection
			} else {
				peer.Opts.Logger.Debug("ignoring connection for expired ephemeral peer")
				peerConn = nil
			}
		} else {
			peer.Opts.Logger.Debug("ignoring connection for unlinked peer")
			peerConn = nil
		}

		if peerConn != nil {
			peer.tearDownConnection(peerConn, remote)

			peerConn.connection = e.connection
			peerConn.gcmSession = e.gcmSession
			peerConn.timestamp = time.Now()

			peer.startConnection(peerConn, remote)
		}

	case dialTimeout:
		peer.Opts.Logger.Debug("dial timeout", zap.String("remote", remote.String()), zap.Error(e.err))

		peer.PeerTable.AddExpiry(remote, peer.Opts.PeerExpiryTimeout)

		expired := peer.PeerTable.HandleExpired(remote)
		if !expired {
			if _, ok := peer.linkedPeers[remote]; ok {
				peer.Opts.Logger.Debug("linked peer, try redialing")

				// This can happen if an ephemeral connection that was still dialling
				// was upgraded to a linked peer.

				remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
				if ok && remoteAddr.Protocol == wire.TCP {
					go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
				}
			} else if _, ok := peer.ephemeralConnections[remote]; ok {
				peer.Opts.Logger.Debug("ephemeral peer, delete")
				delete(peer.ephemeralConnections, remote)
			} else {
				peer.Opts.Logger.Debug("unlinked peer, ignore")
				// Do nothing.
			}
		} else {
			peer.Opts.Logger.Debug("peer expired")
		}

	case linkPeer:
		peer.Opts.Logger.Debug("link peer", zap.String("remote", remote.String()))

		if _, ok := peer.linkedPeers[remote]; ok {
			peer.Opts.Logger.Debug("already linked, ignoring")

			// Do nothing.
			e.errorResponder <- nil
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			peer.Opts.Logger.Debug("upgrading ephemeral connection")

			// Upgrade to a linked peer.

			if len(peer.linkedPeers) >= int(peer.Opts.MaxLinkedPeers) {
				e.errorResponder <- ErrTooManyLinkedPeers
			} else {
				peerConnection := ephemeralConnection.peerConnection
				delete(peer.ephemeralConnections, remote)
				peer.linkedPeers[remote] = &peerConnection

				e.errorResponder <- nil
			}
		} else {
			peer.Opts.Logger.Debug("create new for unlinked peer")

			// Create a new linked peer.
			if len(peer.linkedPeers) >= int(peer.Opts.MaxLinkedPeers) {
				e.errorResponder <- ErrTooManyLinkedPeers
			} else {
				peerConnection := peerConnection{
					connection:       nil,
					outgoingMessages: make(chan wire.Msg, peer.Opts.OutgoingBufferSize),
				}

				remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
				if ok && remoteAddr.Protocol == wire.TCP {
					go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
				}

				peer.linkedPeers[remote] = &peerConnection

				e.errorResponder <- nil
			}
		}

	case discoverPeers:
		peer.Opts.Logger.Debug("discover peers")

		pingData, err := surge.ToBinary(peer.addr)
		if err != nil {
			peer.Opts.Logger.DPanic("marshalling own address", zap.Error(err))
		}
		message := wire.Msg{
			Version: wire.MsgVersion1,
			Type:    wire.MsgTypePing,
			Data:    pingData[:],
		}

		recipients := peer.PeerTable.RandomPeers(peer.Opts.PingAlpha)
		warnThreshold := len(recipients) / 2
		numErrors := 0
		for _, recipient := range recipients {
			peer.Opts.Logger.Debug("sending to peer", zap.String("peer", recipient.String()))
			if err := peer.handleSendMessage(recipient, message); err != nil {
				peer.Opts.Logger.Debug("error sending to peer", zap.String("remote", recipient.String()), zap.Error(err))
				numErrors++
			}
		}

		if numErrors > warnThreshold {
			peer.Opts.Logger.Warn("low ping gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(recipients)-numErrors, len(recipients))))
		}

	case unlinkPeer:
		peer.Opts.Logger.Debug("discover peers", zap.String("remote", remote.String()))

		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peer.Opts.Logger.Debug("linked peer, unlinking")

			// TODO(ross): Should we be calling `tearDownConnection` here
			// instead?
			if linkedPeer.connection != nil {
				linkedPeer.connection.Close()
			}

			delete(peer.linkedPeers, remote)
		} else {
			peer.Opts.Logger.Debug("unlinked/ephemeral peer, ignoring")
			// Do nothing.
		}

	default:
		panic(fmt.Sprintf("unexpected variant: %v", e.ty))
	}
}

func (peer *Peer) gossip(message wire.Msg) {
	peer.Opts.Logger.Debug("gossiping", zap.String("subnet", message.To.String()), zap.String("content id", base64.RawURLEncoding.EncodeToString(message.Data)))

	subnet := id.Hash(message.To)
	var recipients []id.Signatory
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
		peer.Opts.Logger.Debug("gossiping to peer", zap.String("peer", recipient.String()))
		if err := peer.handleSendMessage(recipient, message); err != nil {
			peer.Opts.Logger.Debug("error gossiping to peer", zap.Error(err))
			numErrors++
		}
	}

	if numErrors > warnThreshold {
		peer.Opts.Logger.Warn("low gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(recipients)-numErrors, len(recipients))))
	}
}

func (peer *Peer) hasSpaceForNewGossipSubnet() bool {
	if uint(len(peer.gossipSubnets)) < peer.Opts.MaxGossipSubnets {
		return true
	} else {
		now := time.Now()

		for contentID, gossipSubnet := range peer.gossipSubnets {
			if now.After(gossipSubnet.expiry) {
				delete(peer.gossipSubnets, contentID)
				peer.filter.deny([]byte(contentID))
			}
		}

		return uint(len(peer.gossipSubnets)) < peer.Opts.MaxGossipSubnets
	}
}

func (peer *Peer) handleSendMessage(remote id.Signatory, message wire.Msg) error {
	peer.Opts.Logger.Debug("handling send message", zap.String("remote", remote.String()))

	if linkedPeer, ok := peer.linkedPeers[remote]; ok {
		peer.Opts.Logger.Debug("sending to linked peer")

		ctx, cancel := context.WithTimeout(peer.ctx, peer.Opts.OutgoingBufferTimeout)
		defer cancel()

		select {
		case linkedPeer.outgoingMessages <- message:
			return nil

		case <-ctx.Done():
			if ctx.Err() == nil {
				return ErrMessageBufferFull
			} else {
				return nil
			}
		}
	} else if eConn, ok := peer.ephemeralConnections[remote]; ok {
		peer.Opts.Logger.Debug("sending to ephemeral peer")

		expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)
		eConn.expiryDeadline = expiryDeadline
		if eConn.connection != nil {
			eConn.connection.SetDeadline(eConn.expiryDeadline)
		}
		peer.Opts.Logger.Debug("connection expiry deadline set", zap.Time("deadline", expiryDeadline))

		select {
		case eConn.outgoingMessages <- message:
			return nil

		default:
			return ErrMessageBufferFull
		}
	} else {
		peer.Opts.Logger.Debug("sending to unlinked peer")

		if len(peer.ephemeralConnections) >= int(peer.Opts.MaxEphemeralConnections) {
			return ErrTooManyEphemeralConnections
		} else {
			expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)

			eConn := &ephemeralConnection{
				peerConnection: peerConnection{
					connection:       nil,
					outgoingMessages: make(chan wire.Msg, peer.Opts.OutgoingBufferSize),
				},
				expiryDeadline: expiryDeadline,
			}

			remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
			if ok && remoteAddr.Protocol == wire.TCP {
				ctx, cancel := context.WithTimeout(peer.ctx, peer.Opts.EphemeralConnectionTTL)
				go func() {
					dialAndPublishEvent(ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
					cancel()
				}()
			}

			eConn.outgoingMessages <- message

			peer.ephemeralConnections[remote] = eConn

			peer.Opts.Logger.Debug("added new ephemeral connection", zap.Time("expiry deadline", expiryDeadline))

			return nil
		}
	}
}

func (peer *Peer) tearDownConnection(peerConn *peerConnection, remote id.Signatory) {
	peer.Opts.Logger.Debug("tearing down connection", zap.String("remote", remote.String()))

	if peerConn.connection != nil {
		peer.Opts.Logger.Debug("closing connection")

		peerConn.connection.Close()
		peerConn.connection = nil

		// We create a new channel so that we can signal to the writer to
		// finish if it is blocking on reading from the outgoing message
		// channel.
		newOutgoingBuffer := make(chan wire.Msg, peer.Opts.OutgoingBufferSize)
		n := 0
	LOOP:
		for {
			select {
			case msg := <-peerConn.outgoingMessages:
				newOutgoingBuffer <- msg
				n++

			default:
				break LOOP
			}
		}

		peer.Opts.Logger.Debug(fmt.Sprintf("%v messages transferred to new outgoing message buffer", n))

		close(peerConn.outgoingMessages)
		peerConn.outgoingMessages = newOutgoingBuffer

		// NOTE(ross): Waiting for the reading and writing go routines to finish
		// relies on the fact that closing the connection will cause blocked
		// reads/writes to return, otherwise this will block the whole event loop.
		// This behaviour is sepcified by the documentation for `Close`, so we
		// should be OK.
		<-peerConn.readDone
		peer.Opts.Logger.Debug("reading task done")
		peerConn.pendingMessage = <-peerConn.writeDone
		peer.Opts.Logger.Debug("writing task done")
	} else {
		peer.Opts.Logger.Debug("connection already closed")
	}

}

func (peer *Peer) startConnection(peerConn *peerConnection, remote id.Signatory) {
	peer.Opts.Logger.Debug("starting connection", zap.String("remote", remote.String()))
	peerConn.readDone = make(chan struct{}, 1)
	peerConn.writeDone = make(chan *wire.Msg, 1)

	firstMessage := peerConn.pendingMessage
	peerConn.pendingMessage = nil

	go read(
		peer.ctx,
		peerConn.connection,
		peerConn.gcmSession,
		peer.filter,
		peer.events,
		remote,
		peer.Opts.ConnectionRateLimiterOptions,
		peer.Opts.MaxMessageSize,
		peerConn.readDone,
		peer.Opts.Logger,
	)
	go write(
		peer.ctx,
		peer.Opts.WriteTimeout,
		peerConn.connection,
		peerConn.gcmSession,
		peer.events,
		peerConn.outgoingMessages,
		peer.Opts.MaxMessageSize,
		peerConn.writeDone,
		remote,
		firstMessage,
		peer.Opts.Logger,
	)
}

func dialAndPublishEvent(
	ctx, peerCtx context.Context,
	events chan event,
	privKey *id.PrivKey,
	logger *zap.Logger,
	dialRetryInterval time.Duration,
	remote id.Signatory,
	remoteAddr string,
) {
	logger.Debug("dialing", zap.String("remote", remote.String()), zap.String("address", remoteAddr))
	conn, err := dial(ctx, remoteAddr, dialRetryInterval, logger)

	var e event
	e.id = remote
	if err != nil {
		e.ty = dialTimeout
		e.err = err
	} else {
		gcmSession, discoveredRemote, err := handshake.Handshake(privKey, conn)

		if err != nil {
			logger.Warn("handshake failed", zap.String("dialled remote", remote.String()), zap.Error(err))
			conn.Close()

			e.ty = dialTimeout
			e.err = err
		} else if !remote.Equal(&discoveredRemote) {
			// TODO(ross): What to do here? This being an error probably relies
			// on only using signed addresses during peer discovery.
			logger.Error("handshake derived remote mismatch", zap.String("dialled remote", remote.String()), zap.String("handshake derived remote", discoveredRemote.String()))
		} else {
			logger.Debug("handshake success", zap.String("remote", remote.String()))
			e.ty = newConnection
			e.connection = conn
			e.gcmSession = gcmSession
		}
	}

	select {
	case <-peerCtx.Done():
	case events <- e:
	}
}

func read(
	ctx context.Context,
	conn net.Conn,
	gcmSession *session.GCMSession,
	filter *syncFilter,
	events chan<- event,
	remote id.Signatory,
	rateLimiterOptions RateLimiterOptions,
	maxMessageSize uint,
	done chan<- struct{},
	logger *zap.Logger,
) {
	logger.Debug("read task started", zap.String("remote", remote.String()))

	unmarshalBuffer := make([]byte, maxMessageSize)
	decodeBuffer := make([]byte, maxMessageSize)

	rateLimiter := rate.NewLimiter(rateLimiterOptions.Rate, rateLimiterOptions.Burst)

	addr := conn.RemoteAddr()

	for {
		decodedMessage, err := readAndDecode(conn, gcmSession, rateLimiter, decodeBuffer, unmarshalBuffer)
		if err != nil {
			logger.Debug("read task ending: error reading/decoding message", zap.String("remote", remote.String()))

			e := event{
				ty:  readerDropped,
				id:  remote,
				err: err,
			}

			select {
			case <-ctx.Done():
			case events <- e:
			}

			close(done)
			return
		}

		msg := wire.Msg{}
		_, _, err = msg.Unmarshal(decodedMessage, len(decodedMessage))
		if err != nil {
			logger.Debug("read task ending: error unmarshalling message", zap.String("remote", remote.String()))

			e := event{
				ty:  readerDropped,
				id:  remote,
				err: fmt.Errorf("unmarshalling message: %v", err),
			}

			select {
			case <-ctx.Done():
			case events <- e:
			}

			close(done)
			return
		} else {
			if msg.Type == wire.MsgTypeSync {
				if filter.filter(remote, msg) {
					logger.Debug("read task ending: sync message not accepted by filter", zap.String("remote", remote.String()))

					e := event{
						ty:  readerDropped,
						id:  remote,
						err: fmt.Errorf("unexpected sync for content %v", base64.RawURLEncoding.EncodeToString(msg.Data)),
					}

					select {
					case <-ctx.Done():
					case events <- e:
					}

					close(done)
					return
				}

				decodedSyncData, err := readAndDecode(conn, gcmSession, rateLimiter, decodeBuffer, unmarshalBuffer)
				if err != nil {
					logger.Debug("read task ending: reading/decoding sync data", zap.String("remote", remote.String()))

					e := event{
						ty:  readerDropped,
						id:  remote,
						err: err,
					}

					select {
					case <-ctx.Done():
					case events <- e:
					}

					close(done)
					return
				}

				msg.SyncData = make([]byte, len(decodedSyncData))
				copy(msg.SyncData, decodedSyncData)
			}

			logger.Debug("message received by reader", zap.String("remote", remote.String()))

			e := event{
				ty:      incomingMessage,
				id:      remote,
				message: msg,
				addr:    addr,
			}

			select {
			case <-ctx.Done():
				logger.Debug("read task ending: context expired", zap.String("remote", remote.String()))
				return
			case events <- e:
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
	events chan<- event,
	outgoingMessages chan wire.Msg,
	maxMessageSize uint,
	done chan<- *wire.Msg,
	remote id.Signatory,
	firstMessage *wire.Msg,
	logger *zap.Logger,
) {
	logger.Debug("write task started", zap.Bool("have first message", firstMessage != nil), zap.String("remote", remote.String()))

	marshalBuffer := make([]byte, maxMessageSize)
	encodeBuffer := make([]byte, maxMessageSize)

	var msg wire.Msg
	var ok bool
	for {
		if firstMessage != nil {
			logger.Debug("writer sending first message", zap.String("remote", remote.String()))
			msg, ok = *firstMessage, true
			firstMessage = nil
		} else {
			select {
			case <-ctx.Done():
				logger.Debug("writer task ending: context expired", zap.String("remote", remote.String()))
				return
			case msg, ok = <-outgoingMessages:
				logger.Debug("writer sending outgoing message", zap.String("remote", remote.String()))
			}
		}

		if !ok {
			logger.Debug("writer task ending: outgoing messages channel closed", zap.String("remote", remote.String()))
			close(done)
			return
		} else {
			marshalBuffer = marshalBuffer[:cap(marshalBuffer)]
			encodeBuffer = encodeBuffer[:cap(encodeBuffer)]

			tail, _, err := msg.Marshal(marshalBuffer, len(marshalBuffer))
			if err != nil {
				panic(fmt.Sprintf("marshalling outgoing message: %v", err))
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
				logger.Debug("writer task ending: writing message to connection failed", zap.String("remote", remote.String()))

				done <- &msg
				event := event{
					ty:  writerDropped,
					id:  remote,
					err: err,
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
					logger.Debug("writer task ending: writing sync data to connection failed", zap.String("remote", remote.String()))

					done <- &msg
					event := event{
						ty:  writerDropped,
						id:  remote,
						err: err,
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

func numPendingSyncs(pendingSyncs map[string]pendingSync) uint {
	for id, pSync := range pendingSyncs {
		if pSync.ctx.Err() != nil {
			delete(pendingSyncs, id)
		}
	}
	return uint(len(pendingSyncs))
}
