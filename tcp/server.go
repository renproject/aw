package tcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var DefaultServerOptions = ServerOptions{
	Logger:           logrus.StandardLogger(),
	Timeout:          20 * time.Second,
	Host:             "127.0.0.1:19231",
	RateLimit:        time.Minute,
	TimeoutKeepAlive: 10 * time.Second,
	Handshaker:       nil,
}

type ServerOptions struct {
	Logger           logrus.FieldLogger
	Timeout          time.Duration
	Host             string
	RateLimit        time.Duration
	TimeoutKeepAlive time.Duration
	Handshaker       handshake.Handshaker

	// TODO: Implement a maximum number of connections to help protect the
	// server from DoS attacks.

	// TODO: Implement IP blacklisting to help protect the server from DoS
	// attacks.
}

func (options *ServerOptions) setZerosToDefaults() {
	if options.Logger == nil {
		options.Logger = DefaultServerOptions.Logger
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultServerOptions.Timeout
	}
	if options.TimeoutKeepAlive == 0 {
		options.TimeoutKeepAlive = DefaultServerOptions.TimeoutKeepAlive
	}
	if options.Host == "" {
		options.Host = DefaultServerOptions.Host
	}
	if options.RateLimit == 0 {
		options.RateLimit = DefaultServerOptions.RateLimit
	}
}

type Server struct {
	options  ServerOptions
	messages protocol.MessageSender

	lastConnAttemptsMu *sync.RWMutex
	lastConnAttempts   map[string]time.Time
}

func NewServer(options ServerOptions, messages protocol.MessageSender) *Server {
	options.setZerosToDefaults()
	return &Server{
		options:  options,
		messages: messages,

		lastConnAttemptsMu: new(sync.RWMutex),
		lastConnAttempts:   map[string]time.Time{},
	}
}

// Run the server until the context is done. The server will continuously listen
// for new connections, spawning each one into a background goroutine so that it
// can be handled concurrently.
func (server *Server) Run(ctx context.Context) {
	listener, err := net.Listen("tcp", server.options.Host)
	if err != nil {
		server.options.Logger.Errorf("failed to listen on %s: %v", server.options.Host, err)
		return
	}

	go func() {
		// When the context is done, explicitly close the listener so that it
		// does not block on waiting to accept a new connection.
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.options.Logger.Errorf("error closing listener: %v", err)
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

			server.options.Logger.Errorf("error accepting connection: %v", err)
			continue
		}

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections.
		go server.handle(ctx, conn)
	}
}

func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	if !server.allowRateLimit(conn) {
		// Reject connections from IP addresses that have attempted to connect
		// too recently.
		return
	}

	// Attempt to establish a session with the client. A `nil` session can be
	// returned by this method.
	session, err := server.establishSession(ctx, conn)
	if err != nil {
		server.options.Logger.Warn("closing connection: error establishing session: %v", err)
		return
	}

	for {
		messageOtw := protocol.MessageOnTheWire{}
		if session != nil {
			var err error
			messageOtw, err = session.ReadMessageOnTheWire(conn)
			if err != nil {
				if err != io.EOF {
					server.options.Logger.Error(newErrReadingIncomingMessage(err))
				}
				server.options.Logger.Info("closing connection: EOF")
				return
			}
		} else {
			var message protocol.Message
			if err := message.UnmarshalReader(conn); err != nil {
				if err != io.EOF {
					server.options.Logger.Error(newErrReadingIncomingMessage(err))
				}
				server.options.Logger.Info("closing connection: EOF")
				return
			}
			messageOtw.Message = message
		}

		select {
		case <-ctx.Done():
			return
		case server.messages <- messageOtw:
		}
	}
}

func (server *Server) allowRateLimit(conn net.Conn) bool {
	server.lastConnAttemptsMu.Lock()
	defer server.lastConnAttemptsMu.Unlock()
	defer func() {
		server.lastConnAttempts[conn.RemoteAddr().String()] = time.Now()
	}()

	lastConnAttempt, ok := server.lastConnAttempts[conn.RemoteAddr().String()]
	if !ok {
		return true
	}

	if time.Now().Sub(lastConnAttempt) < server.options.RateLimit {
		server.options.Logger.Warn("%s is rate limited", conn.RemoteAddr())
		return false
	}
	return true
}

func (server *Server) establishSession(ctx context.Context, conn net.Conn) (protocol.Session, error) {
	if server.options.Handshaker == nil {
		return nil, nil
	}

	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, server.options.Timeout)
	defer handshakeCancel()

	session, err := server.options.Handshaker.AcceptHandshake(handshakeCtx, conn)
	if err != nil {
		return nil, fmt.Errorf("bad handshake with %v: %v", conn.RemoteAddr().String(), err)
	}
	return session, nil
}

type ErrReadingIncomingMessage struct {
	error
}

func newErrReadingIncomingMessage(err error) error {
	return ErrReadingIncomingMessage{fmt.Errorf("error reading incoming message: %v", err)}
}
