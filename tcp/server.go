package tcp

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var DefaultServerOptions = ServerOptions{
	Logger:  logrus.StandardLogger(),
	Timeout: time.Second,
	Host:    "127.0.0.1:19231",

	Handshaker: nil,
}

type ServerOptions struct {
	Logger  logrus.FieldLogger
	Timeout time.Duration
	Host    string
	// ReconnRateLimit is the time the darknode waits for before accepting a connection
	// from the same peer. Default: 1 minute
	ReconnRateLimit time.Duration
	// Handshaker handles the handshake process between peers. Default: no handshake
	Handshaker handshake.Handshaker
}

type Server struct {
	mu              *sync.RWMutex
	lastConnAttempt map[string]time.Time

	options  ServerOptions
	messages protocol.MessageSender
}

func NewServer(options ServerOptions, messages protocol.MessageSender) *Server {
	if options.Host == "" {
		options.Host = DefaultServerOptions.Host
	}

	if options.Logger == nil {
		logger := logrus.New()
		logger.SetOutput(ioutil.Discard)
		options.Logger = logger
	}

	if options.Timeout == time.Duration(0) {
		options.Timeout = DefaultServerOptions.Timeout
	}

	if options.ReconnRateLimit == 0 {
		options.ReconnRateLimit = time.Minute
	}

	return &Server{
		mu:              new(sync.RWMutex),
		lastConnAttempt: map[string]time.Time{},
		options:         options,
		messages:        messages,
	}
}

func (server *Server) Run(ctx context.Context) {
	listener, err := net.Listen("tcp", server.options.Host)
	if err != nil {
		server.options.Logger.Errorf("failed to listen on %s: %v", server.options.Host, err)
		return
	}

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.options.Logger.Errorf("error closing tcp listener: %v", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check whether or not the context is done
			select {
			case <-ctx.Done():
				return
			default:
			}

			server.options.Logger.Errorf("error accepting tcp connection: %v", err)
			continue
		}

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections
		go server.handle(ctx, conn)
	}
}

func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	server.mu.Lock()
	if time.Now().Sub(server.lastConnAttempt[conn.RemoteAddr().String()]) < server.options.ReconnRateLimit {
		server.options.Logger.Info("last attempt from: %s is less than the minimum reconnect delay", conn.RemoteAddr())
		server.mu.Unlock()
		return
	}
	server.lastConnAttempt[conn.RemoteAddr().String()] = time.Now()
	server.mu.Unlock()

	var session protocol.Session
	var err error
	if server.options.Handshaker != nil {
		handshakeCtx, handshakeCancel := context.WithTimeout(ctx, server.options.Timeout)
		defer handshakeCancel()
		session, err = server.options.Handshaker.AcceptHandshake(handshakeCtx, conn)
		if err != nil {
			server.options.Logger.Errorf("bad handshake with %v: %v", conn.RemoteAddr().String(), err)
			return
		}
	}

	// Accepted connections are not written to, so we prevent write timeouts by
	// setting the write deadline to zero
	conn.SetWriteDeadline(time.Time{})

	// We do not know when the client will send us messages, so we prevent rea
	// timeouts by setting the read deadline to zero
	conn.SetReadDeadline(time.Time{})

	for {
		messageOtw := protocol.MessageOnTheWire{}
		if session != nil {
			var err error
			messageOtw, err = session.ReadMessage(conn)
			if err != nil {
				if err != io.EOF {
					server.options.Logger.Error(newErrReadingIncomingMessage(err))
				}
				return
			}
		} else {
			msg, err := protocol.ReadMessage(conn)
			if err != nil {
				if err != io.EOF {
					server.options.Logger.Error(newErrReadingIncomingMessage(err))
				}
				return
			}
			messageOtw.Message = msg
		}

		select {
		case <-ctx.Done():
			return
		case server.messages <- messageOtw:
		}
	}
}

type ErrReadingIncomingMessage struct {
	error
}

func newErrReadingIncomingMessage(err error) error {
	return ErrReadingIncomingMessage{fmt.Errorf("error reading incoming message: %v", err)}
}
