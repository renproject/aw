package tcp

import (
	"bytes"
	"context"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/message"
	"github.com/sirupsen/logrus"
)

var (
	DefaultClientHandshaker  = handshake.NewInsecure()
	DefaultClientTimeout     = 10 * time.Second
	DefaultClientTimeToLive  = 24 * time.Hour
	DefaultClientMaxCapacity = 1024
	DefaultClientMaxConns    = 256
)

// ClientOptions are used to parameterize the behaviour of the Client.
type ClientOptions struct {
	// Logger for all information/debugging/error output.
	Logger logrus.FieldLogger
	// Handshaker for establishing an authenticated and encrypted session with
	// the server. Typically, assymetric authentication and encryption is used
	// to establish ephemeral symmetric authenticatoin and encryption for the
	// life of the connection.
	Handshaker handshake.Handshaker
	// Timeout used when dialing new connections.
	Timeout time.Duration
	// TimeToLive for maintaining connections for re-use.
	TimeToLive time.Duration
	// MaxCapacity of messages that can be bufferred while waiting for a
	// connection to dial successfully.
	MaxCapacity int
	// MaxConns that can be open at any time. When it is exceeded, the least
	// recently used connections are closed.
	MaxConns int
}

func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Logger:      logrus.New(),
		Handshaker:  DefaultClientHandshaker,
		Timeout:     DefaultClientTimeout,
		TimeToLive:  DefaultClientTimeToLive,
		MaxCapacity: DefaultClientMaxCapacity,
		MaxConns:    DefaultClientMaxConns,
	}
}

func (opts ClientOptions) WithLogger(logger logrus.FieldLogger) ClientOptions {
	opts.Logger = logger
	return opts
}

func (opts ClientOptions) WithHandshaker(handshaker handshake.Handshaker) ClientOptions {
	opts.Handshaker = handshaker
	return opts
}

func (opts ClientOptions) WithTimeout(timeout time.Duration) ClientOptions {
	opts.Timeout = timeout
	return opts
}

func (opts ClientOptions) WithTimeToLive(ttl time.Duration) ClientOptions {
	opts.TimeToLive = ttl
	return opts
}

func (opts ClientOptions) WithMaxCapacity(capacity int) ClientOptions {
	opts.MaxCapacity = capacity
	return opts
}

func (opts ClientOptions) WithMaxConns(maxConns int) ClientOptions {
	opts.MaxConns = maxConns
	return opts
}

type Client interface {
	// Send a message to the network address.
	Send(context.Context, string, message.Message) error
	// Close all connections to the network address.
	Close(string)
	// CloseAll all connections to all network addresses.
	CloseAll()
}

type cancelConn struct {
	cancel   context.CancelFunc
	messages chan<- message.Message
}

type client struct {
	opts ClientOptions

	connMu *sync.Mutex
	conns  map[string]cancelConn
}

func NewClient(opts ClientOptions) Client {
	client := &client{
		opts: opts,

		connMu: new(sync.Mutex),
		conns:  make(map[string]cancelConn, opts.MaxConns),
	}
	return client
}

func (client *client) Send(ctx context.Context, address string, m message.Message) error {
	client.connMu.Lock()
	defer client.connMu.Unlock()

	// Get an existing connection, or dial a new connection. Dialing a new
	// connection is non-blocking, and will immediately return a messaging
	// channel that can be used to buffer messages until the connection is
	// successfully dialed.
	conn, ok := client.conns[address]
	if !ok {
		// Notice that the context used for dialing the connection is disjoint
		// from the context used for sending the message. This is intentional,
		// as it allows the connection to be long-lived and re-used for future
		// messages.
		ctx, cancel := context.WithCancel(context.Background())
		conn = cancelConn{
			cancel: cancel, // Store the cancel function. We must guarantee that it is always called.
			messages: dial(
				ctx,                     // Parent context that can cancel the connection.
				client.opts.Handshaker,  // Handshaker for establishing authentication/encryption.
				client.opts.Timeout,     // Maximum time that the connection will wait for an initial connection attempt to succeed.
				client.opts.TimeToLive,  // Maximum time that the connection will stay alive.
				client.opts.MaxCapacity, // Maximum number of messages that will be buffered on the connection before it starts to drop new incoming messages.
				address,                 // Network address that will be dialed.
				client.shutdown,         // Callback for catching when the connection gets shutdown.
				client.errorf,           // Callback for formatting and logging errors.
			),
		}
		client.conns[address] = conn
	}

	// Send the message to the dialed connection.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case conn.messages <- m:
		return nil
	}
}

func (client *client) Close(address string) {
	client.shutdown(address)
}

func (client *client) CloseAll() {
	client.connMu.Lock()
	defer client.connMu.Unlock()

	// Cancel all connections and then remove all of them from the map. This is
	// more efficient than removing them one-by-one.
	for address, conn := range client.conns {
		conn.cancel()
		delete(client.conns, address)
	}
}

func (client *client) shutdown(address string) {
	client.connMu.Lock()
	defer client.connMu.Unlock()

	// Remove the connection from the connection pool and cancel its context, to
	// make sure that it exits its loop.
	if conn, ok := client.conns[address]; ok {
		conn.cancel()
		delete(client.conns, address)
	}
}

func (client *client) errorf(format string, args ...interface{}) {
	client.opts.Logger.Errorf(format, args)
}

func dial(ctx context.Context, handshaker handshake.Handshaker, timeout, ttl time.Duration, maxCap int, address string, shutdown func(string), errorf func(string, ...interface{})) chan<- message.Message {
	messages := make(chan message.Message, maxCap)

	go func() {
		defer shutdown(address)

		// Dial a new connection. During this time, messages will buffer in the
		// messages channel until the channel is full. Once the channel is full,
		// new messages will be dropped and lost.
		innerCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		conn, err := new(net.Dialer).DialContext(innerCtx, "tcp", address)
		if err != nil {
			errorf("dial connection: %v", err)
			return
		}
		defer func() {
			if err := conn.Close(); err != nil {
				errorf("close connection: %v", err)
			}
		}()

		// Handshake with the server to establish authentication and encryption
		// on the connection. This will return a decorated network connection —
		// e.g. a session — that will automatically authenticate/encrypt
		// messages. The original network connection should not be directly used
		// after the handshake. Notice that the initial dialing timeout includes
		// the time taken to complete the handshake.
		session, err := handshaker.Handshake(innerCtx, conn)
		if err != nil {
			errorf("bad handshake: %v", err)
			return
		}
		if session == nil {
			errorf("bad session: nil")
			return
		}

		// Read messages and write them to the connection until the context is
		// cancelled.
		innerCtx, cancel = context.WithTimeout(ctx, ttl)
		defer cancel()

		// Read bufferred messages and marshal them directly to the underlying
		// network connection. Any errors that are encountered will result in
		// the shutdown of the connection, and the loss of all bufferred
		// messages.
		buf := new(bytes.Buffer)
		buf.Grow(1024 * 1024) // Pre-allocate 1MB of space for message writing.
		for {
			select {
			case <-innerCtx.Done():
				errorf("kill connection: %v", innerCtx.Err())
				return
			case m := <-messages:
				// Encrypt message body.
				m.Data, err = session.Encrypt(m.Data)
				if err != nil {
					errorf("encrypt: %v", err)
					continue
				}

				// Write encrypted message to connection.
				buf.Reset()
				if err := m.Marshal(buf); err != nil {
					errorf("write to buffer: %v", err)
					continue
				}
				if _, err := conn.Write(buf.Bytes()); err != nil {
					errorf("write to connection: %v", err)
					continue
				}
			}
		}
	}()

	return messages
}
