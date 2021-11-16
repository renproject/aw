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

	id      id.Signatory
	privKey *id.PrivKey
	Port    uint16

	receive func(id.Signatory, []byte)

	ctx                  context.Context
	events               chan event
	linkedPeers          map[id.Signatory]*peerConnection
	ephemeralConnections map[id.Signatory]*ephemeralConnection
	pendingSyncs         map[string]pendingSync
	gossipSubnets        map[string]gossipSubnet
	filter               *syncFilter

	PeerTable       dht.Table
	ContentResolver dht.ContentResolver
}

func New(opts Options, privKey *id.PrivKey, peerTable dht.Table, contentResolver dht.ContentResolver, receive func(id.Signatory, []byte)) *Peer {
	self := privKey.Signatory()

	events := make(chan event, opts.EventLoopBufferSize)
	linkedPeers := make(map[id.Signatory]*peerConnection, opts.MaxLinkedPeers)
	ephemeralConnections := make(map[id.Signatory]*ephemeralConnection, opts.MaxEphemeralConnections)
	pendingSyncs := make(map[string]pendingSync, opts.MaxPendingSyncs)
	gossipSubnets := make(map[string]gossipSubnet, opts.MaxGossipSubnets)

	filter := newSyncFilter()

	return &Peer{
		Opts: opts,

		id:      self,
		privKey: privKey,
		Port:    0,

		receive: receive,

		ctx:                  nil,
		events:               events,
		linkedPeers:          linkedPeers,
		ephemeralConnections: ephemeralConnections,
		pendingSyncs:         pendingSyncs,
		gossipSubnets:        gossipSubnets,

		PeerTable:       peerTable,
		ContentResolver: contentResolver,
		filter:          filter,
	}
}

func (peer *Peer) ID() id.Signatory {
	return peer.privKey.Signatory()
}

func (peer *Peer) Receive(receive func(id.Signatory, []byte)) {
	peer.receive = receive
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

	select {
	case peer.events <- event:
		return nil

	default:
		return ErrEventLoopFull
	}
}

func (peer *Peer) Sync(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	event, errResponder, responder := syncEvent(ctx, contentID, hint)

	select {
	case peer.events <- event:

	default:
		return nil, ErrEventLoopFull
	}

	return syncResponse(ctx, errResponder, responder)
}

func (peer *Peer) SyncNonBlocking(ctx context.Context, contentID []byte, hint *id.Signatory) ([]byte, error) {
	event, errResponder, responder := syncEvent(ctx, contentID, hint)

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

	select {
	case peer.events <- event:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (peer *Peer) GossipNonBlocking(contentID []byte, subnet *id.Hash) error {
	event := gossipEvent(contentID, subnet)

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
		peer.events <- newConnectionEvent
	}()
}

func (peer *Peer) handleEvent(e event) {
	peer.Opts.Logger.Debug("handling event", zap.String("self", peer.id.String()[:4]), zap.String("type", e.ty.String()))
	remote := e.id

	switch e.ty {
	case incomingMessage:
		message := e.message

		switch e.message.Type {
		case wire.MsgTypePush:
			if len(message.Data) != 0 {
				if _, ok := peer.ContentResolver.QueryContent(message.Data); !ok {
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

			if !peer.filter.filter(remote, message) {
				if pendingSync, ok := peer.pendingSyncs[contentID]; ok {
					for _, responder := range pendingSync.responders {
						responder <- message.SyncData
					}

					delete(peer.pendingSyncs, contentID)
				}

				if len(message.Data) != 0 && len(message.SyncData) != 0 {
					_, alreadySeenContent := peer.ContentResolver.QueryContent(message.Data)

					if !alreadySeenContent {
						peer.ContentResolver.InsertContent(message.Data, message.SyncData)
					}
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
			}

		case wire.MsgTypeSend:
			if peer.receive != nil {
				peer.receive(remote, e.message.Data)
			}

		case wire.MsgTypePing:
			if dataLen := len(message.Data); dataLen != 2 {
				peer.Opts.Logger.Warn("malformed port", zap.String("peer", remote.String()), zap.Uint64("port byte size", uint64(dataLen)))
			}
			port := binary.LittleEndian.Uint16(message.Data)

			peer.PeerTable.AddPeer(
				remote,
				wire.NewUnsignedAddress(wire.TCP, fmt.Sprintf("%v:%v", e.addr.(*net.TCPAddr).IP.String(), port), uint64(time.Now().UnixNano())),
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
			} else {
				for _, signatoryAndAddr := range signatoriesAndAddrs {
					// NOTE(ross): We rely on the fact that the peer table won't
					// add itself.
					peer.PeerTable.AddPeer(signatoryAndAddr.Signatory, signatoryAndAddr.Address)
				}
			}

		default:
			peer.Opts.Logger.Warn("unsupported messge type", zap.Uint16("type", message.Type))
		}

	case sendMessage:
		e.errorResponder <- peer.handleSendMessage(remote, e.message)

	case gossipMessage:
		peer.gossip(e.message)

	case syncRequest:
		contentID := e.message.Data

		if pending, ok := peer.pendingSyncs[string(contentID)]; ok {
			if uint(len(pending.responders)) >= peer.Opts.MaxActiveSyncsForSameContent {
				e.errorResponder <- ErrTooManySyncsForSameContent
			} else {
				pending.responders = append(pending.responders, e.messageResponder)
			}
		} else {
			peer.filter.allow(contentID)

			if uint(len(peer.pendingSyncs)) >= peer.Opts.MaxPendingSyncs {
				e.errorResponder <- ErrTooManyPendingSyncs
			} else {
				pending := pendingSync{
					ctx:        e.ctx,
					responders: []chan<- []byte{e.messageResponder},
				}

				peer.pendingSyncs[string(contentID)] = pending
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
				if recipient.Equal(e.hint) {
					peer.Opts.Logger.Warn("unable to sync from hinted peer", zap.String("peer", recipient.String()), zap.Error(err))
				}
				numErrors++
			}
		}

		if numErrors > warnThreshold {
			peer.Opts.Logger.Warn("low sync gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(peers)-numErrors, len(peers))))
		}

	case readerDropped:
		peer.Opts.Logger.Debug("reader dropped", zap.String("self", peer.id.String()[:4]), zap.Error(e.err))
		// TODO(ross): If the error was malicious we should act accordingly.

		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peer.tearDownConnection(linkedPeer)

			remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
			if ok && remoteAddr.Protocol == wire.TCP {
				go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
			}
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			peer.tearDownConnection(&ephemeralConnection.peerConnection)
			if ephemeralConnection.pendingMessage != nil {
				peer.Opts.Logger.Warn("ephemeral connection dropped with unsent message", zap.String("peer", remote.String()))
			}
			delete(peer.ephemeralConnections, remote)
		} else {
			// Do nothing.
		}

	case writerDropped:
		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peer.tearDownConnection(linkedPeer)

			remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
			if ok && remoteAddr.Protocol == wire.TCP {
				go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
			}
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			peer.tearDownConnection(&ephemeralConnection.peerConnection)
			if ephemeralConnection.pendingMessage != nil {
				peer.Opts.Logger.Warn("ephemeral connection dropped with unsent message", zap.String("peer", remote.String()))
			}
			delete(peer.ephemeralConnections, remote)
		} else {
			// Do nothing.
		}

	case newConnection:
		var peerConn *peerConnection
		var wouldKeepAlive bool
		isLinked := false
		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			isLinked = true
			peerConn = linkedPeer
			wouldKeepAlive = linkedPeer.connection == nil || time.Since(linkedPeer.timestamp) > peer.Opts.MinimumConnectionExpiryAge
		} else if eConn, ok := peer.ephemeralConnections[remote]; ok {
			peerConn = &eConn.peerConnection
			wouldKeepAlive = eConn.connection == nil && time.Now().Before(eConn.expiryDeadline)
		} else {
			if uint(len(peer.ephemeralConnections)) < peer.Opts.MaxEphemeralConnections {
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
				peerConn = nil
				wouldKeepAlive = false
			}
		}

		if peerConn != nil {
			cmp := bytes.Compare(peer.id[:], remote[:])
			if cmp == 0 {
				peer.Opts.Logger.DPanic("connection to self", zap.String("self", peer.id.String()))
			} else if cmp > 0 {
				decisionBuffer := [128]byte{}
				var decisionEncoded []byte
				if wouldKeepAlive {
					peer.tearDownConnection(peerConn)

					peerConn.connection = e.connection
					peerConn.gcmSession = e.gcmSession
					peerConn.timestamp = time.Now()

					decisionEncoded = encode([]byte{keepAliveTrue}, decisionBuffer[:], e.gcmSession)

					peer.Opts.Logger.Debug("signalling to keep alive", zap.String("self", peer.id.String()[:4]), zap.String("remote", remote.String()[:4]))
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
					_, err := e.connection.Write(decisionEncoded[:])
					if err != nil {
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
					// TODO(ross): Should this timeout be configurable?
					e.connection.SetDeadline(time.Now().Add(5 * time.Second))

					decisionEncoded = encode([]byte{keepAliveFalse}, decisionBuffer[:], e.gcmSession)

					peer.Opts.Logger.Debug("signalling to drop", zap.String("self", peer.id.String()[:4]), zap.String("remote", remote.String()[:4]))
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
					readBuffer := [128]byte{}
					decisionBuffer := [1]byte{}
					decisionDecoded, err := readAndDecode(e.connection, e.gcmSession, nil, readBuffer[:], decisionBuffer[:])
					if err != nil {
					} else {
						peer.Opts.Logger.Debug("received keep alive message", zap.String("self", peer.id.String()[:4]), zap.String("remote", remote.String()[:4]), zap.Uint8("decision", decisionDecoded[0]))
					}

					if err == nil && decisionDecoded[0] == keepAliveTrue {
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
		var peerConn *peerConnection
		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			peerConn = linkedPeer
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
			if time.Now().Before(ephemeralConnection.expiryDeadline) {
				peerConn = &ephemeralConnection.peerConnection
			} else {
				peerConn = nil
			}
		} else {
			peerConn = nil
		}

		if peerConn != nil {
			peer.tearDownConnection(peerConn)

			peerConn.connection = e.connection
			peerConn.gcmSession = e.gcmSession
			peerConn.timestamp = time.Now()

			peer.startConnection(peerConn, remote)
		}

	case dialTimeout:
		peer.PeerTable.AddExpiry(remote, peer.Opts.PeerExpiryTimeout)

		expired := peer.PeerTable.HandleExpired(remote)
		if !expired {
			if _, ok := peer.linkedPeers[remote]; ok {
				// This can happen if an ephemeral connection that was still dialling
				// was upgraded to a linked peer.

				remoteAddr, ok := peer.PeerTable.PeerAddress(remote)
				if ok && remoteAddr.Protocol == wire.TCP {
					go dialAndPublishEvent(peer.ctx, peer.ctx, peer.events, peer.privKey, peer.Opts.Logger, peer.Opts.DialRetryInterval, remote, remoteAddr.Value)
				}
			} else if _, ok := peer.ephemeralConnections[remote]; ok {
				delete(peer.ephemeralConnections, remote)
			} else {
				// Do nothing.
			}
		}

	case linkPeer:
		if _, ok := peer.linkedPeers[remote]; ok {
			// Do nothing.
			e.errorResponder <- nil
		} else if ephemeralConnection, ok := peer.ephemeralConnections[remote]; ok {
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
		recipients := peer.PeerTable.RandomPeers(peer.Opts.PingAlpha)
		warnThreshold := len(recipients) / 2
		numErrors := 0
		for _, recipient := range recipients {
			if err := peer.handleSendMessage(recipient, e.message); err != nil {
				numErrors++
			}
		}

		if numErrors > warnThreshold {
			peer.Opts.Logger.Warn("low ping gossip success rate", zap.String("proportion of successful sends", fmt.Sprintf("%v/%v", len(recipients)-numErrors, len(recipients))))
		}

	case unlinkPeer:
		if linkedPeer, ok := peer.linkedPeers[remote]; ok {
			if linkedPeer.connection != nil {
				linkedPeer.connection.Close()
			}

			delete(peer.linkedPeers, remote)
		} else {
			// Do nothing.
		}

	default:
		panic(fmt.Sprintf("unexpected variant: %v", e.ty))
	}
}

func (peer *Peer) gossip(message wire.Msg) {
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
		if err := peer.handleSendMessage(recipient, message); err != nil {
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
	if linkedPeer, ok := peer.linkedPeers[remote]; ok {
		ctx, cancel := context.WithTimeout(peer.ctx, peer.Opts.OutgoingBufferTimeout)
		defer cancel()

		select {
		case linkedPeer.outgoingMessages <- message:
			return nil

		case <-ctx.Done():
			if peer.ctx.Err() == nil {
				peer.Opts.Logger.Warn("outgoing message buffer back pressure")
				return ErrMessageBufferFull
			} else {
				return nil
			}
		}
	} else if eConn, ok := peer.ephemeralConnections[remote]; ok {
		expiryDeadline := time.Now().Add(peer.Opts.EphemeralConnectionTTL)
		eConn.expiryDeadline = expiryDeadline
		if eConn.connection != nil {
			eConn.connection.SetDeadline(eConn.expiryDeadline)
		}

		select {
		case eConn.outgoingMessages <- message:
			return nil

		default:
			return ErrMessageBufferFull
		}
	} else {
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

			return nil
		}
	}
}

func (peer *Peer) tearDownConnection(peerConn *peerConnection) {
	peer.Opts.Logger.Debug("tearing down connection", zap.String("self", peer.id.String()[:4]))
	if peerConn.connection != nil {
		peerConn.connection.Close()
		peerConn.connection = nil

		// We create a new channel so that we can signal to the writer to
		// finish if it is blocking on reading from the outgoing message
		// channel.
		newOutgoingBuffer := make(chan wire.Msg, peer.Opts.OutgoingBufferSize)
	LOOP:
		for {
			select {
			case msg := <-peerConn.outgoingMessages:
				newOutgoingBuffer <- msg

			default:
				break LOOP
			}
		}
		close(peerConn.outgoingMessages)
		peerConn.outgoingMessages = newOutgoingBuffer

		// NOTE(ross): Waiting for the reading and writing go routines to finish
		// relies on the fact that closing the connection will cause blocked
		// reads/writes to return, otherwise this will block the whole event loop.
		// This behaviour is sepcified by the documentation for `Close`, so we
		// should be OK.
		<-peerConn.readDone
		peerConn.pendingMessage = <-peerConn.writeDone
	}

}

func (peer *Peer) startConnection(peerConn *peerConnection, remote id.Signatory) {
	peer.Opts.Logger.Debug("starting connection", zap.String("self", peer.id.String()[:4]))
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
	conn, err := dial(ctx, remoteAddr, dialRetryInterval, logger)

	var e event
	e.id = remote
	if err != nil {
		e.ty = dialTimeout
		e.err = err
	} else {
		gcmSession, discoveredRemote, err := handshake.Handshake(privKey, conn)

		if err != nil {
			logger.Warn("handshake failed", zap.Error(err))
			conn.Close()

			e.ty = dialTimeout
			e.err = err
		} else if !remote.Equal(&discoveredRemote) {
			// TODO(ross): What to do here? This being an error probably relies
			// on only using signed addresses during peer discovery.
		} else {
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
) {
	unmarshalBuffer := make([]byte, maxMessageSize)
	decodeBuffer := make([]byte, maxMessageSize)

	rateLimiter := rate.NewLimiter(rateLimiterOptions.Rate, rateLimiterOptions.Burst)

	addr := conn.RemoteAddr()

	for {
		decodedMessage, err := readAndDecode(conn, gcmSession, rateLimiter, decodeBuffer, unmarshalBuffer)
		if err != nil {
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

			e := event{
				ty:      incomingMessage,
				id:      remote,
				message: msg,
				addr:    addr,
			}

			select {
			case <-ctx.Done():
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
