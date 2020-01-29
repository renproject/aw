package tcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Client struct {
	logger logrus.FieldLogger
	pool   ConnPool
}

func NewClient(logger logrus.FieldLogger, pool ConnPool) *Client {
	return &Client{
		logger: logger,
		pool:   pool,
	}
}

func (client *Client) Run(ctx context.Context, messages protocol.MessageReceiver) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageOtw := <-messages:
			go client.handleMessageOnTheWire(messageOtw)
		}
	}
}

func (client *Client) handleMessageOnTheWire(message protocol.MessageOnTheWire) {
	for i := 0; i < 5; i++ {
		err := client.pool.Send(message.To.NetworkAddress(), message.Message)
		if err == nil {
			return
		}
		client.logger.Debugf("error send %v message to %v: %v", message.Message.Variant, message.To.NetworkAddress(), err)
		time.Sleep(time.Second)
	}
}

type ServerOptions struct {
	Host           string        // Host address
	Timeout        time.Duration // Timeout when establish a connection
	RateLimit      time.Duration // Minimum time interval before accepting connection from same peer.
	MaxConnections int           // Max connections allowed.
}

func (options *ServerOptions) setZerosToDefaults() {
	if options.Host == "" {
		options.Host = "127.0.0.1:19231"
	}
	if options.Timeout == 0 {
		options.Timeout = 20 * time.Second
	}
	if options.RateLimit == 0 {
		options.RateLimit = time.Second
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = 256
	}
}

type Server struct {
	logger      logrus.FieldLogger
	options     ServerOptions
	handshaker  handshake.Handshaker
	connections int64

	lastConnAttemptsMu *sync.RWMutex
	lastConnAttempts   map[string]time.Time
}

func NewServer(options ServerOptions, logger logrus.FieldLogger, handshaker handshake.Handshaker) *Server {
	if logger == nil {
		logger = logrus.New()
	}
	options.setZerosToDefaults()
	if handshaker == nil {
		panic("handshaker cannot be nil")
	}
	return &Server{
		logger:      logger,
		options:     options,
		handshaker:  handshaker,
		connections: 0,

		lastConnAttemptsMu: new(sync.RWMutex),
		lastConnAttempts:   map[string]time.Time{},
	}
}

// Run the server until the context is done. The server will continuously listen
// for new connections, spawning each one into a background goroutine so that it
// can be handled concurrently.
func (server *Server) Run(ctx context.Context, messages protocol.MessageSender) {
	server.logger.Debugf("server start listening at %v", server.options.Host)
	listener, err := net.Listen("tcp", server.options.Host)
	if err != nil {
		server.logger.Fatalf("failed to listen on %s: %v", server.options.Host, err)
		return
	}

	go func() {
		// When the context is done, explicitly close the listener so that it
		// does not block on waiting to accept a new connection.
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.logger.Errorf("error closing listener: %v", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Do not log errors because returning from this canceling a
				// context is the expected way to terminate the run loop.
				return
			default:
			}

			server.logger.Errorf("error accepting connection: %v", err)
			continue
		}
		if atomic.LoadInt64(&server.connections) >= int64(server.options.MaxConnections) {
			server.logger.Info("tcp server reaches max number of connections")
			conn.Close()
			continue
		}
		atomic.AddInt64(&server.connections, 1)

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections.

		go server.handle(ctx, conn, messages)
	}
}

func (server *Server) handle(ctx context.Context, conn net.Conn, messages protocol.MessageSender) {
	defer atomic.AddInt64(&server.connections, -1)
	defer conn.Close()

	// Reject connections from IP addresses that have attempted to connect too recently.
	if !server.allowRateLimit(conn) {
		return
	}

	// Attempt to establish a session with the client.
	now := time.Now()
	session, err := server.establishSession(ctx, conn)
	if err != nil {
		server.logger.Errorf("closing connection: error establishing session: %v", err)
		return
	}
	if session == nil {
		server.logger.Errorf("cannot establish session with %v", conn.RemoteAddr().String())
		return
	}
	server.logger.Debugf("new connection with %v takes %v", conn.RemoteAddr().String(), time.Now().Sub(now))

	for {
		messageOtw, err := session.ReadMessageOnTheWire(conn)

		if err != nil {
			if err != io.EOF {
				server.logger.Errorf("error reading incoming message: %v", err)
			}
			server.logger.Info("closing connection: EOF")
			return
		}

		select {
		case <-ctx.Done():
			return
		case messages <- messageOtw:
		}
	}
}

func (server *Server) allowRateLimit(conn net.Conn) bool {
	server.lastConnAttemptsMu.Lock()
	defer server.lastConnAttemptsMu.Unlock()

	var address string
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		address = addr.IP.String()
	} else {
		address = conn.RemoteAddr().String()
	}
	defer func() {
		server.lastConnAttempts[address] = time.Now()
	}()

	lastConnAttempt, ok := server.lastConnAttempts[address]
	if !ok {
		return true
	}

	return time.Now().Sub(lastConnAttempt) >= server.options.RateLimit
}

func (server *Server) establishSession(ctx context.Context, conn net.Conn) (protocol.Session, error) {
	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, server.options.Timeout)
	defer handshakeCancel()

	// Set a timeout for the handshake process
	deadline := time.Now().Add(server.options.Timeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	session, err := server.handshaker.AcceptHandshake(handshakeCtx, conn)
	if err != nil {
		return nil, fmt.Errorf("bad handshake with %v: %v", conn.RemoteAddr().String(), err)
	}

	// Reset the timeout back
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}

	return session, nil
}
