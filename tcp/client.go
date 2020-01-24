package tcp

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/message"
	"github.com/renproject/bound"
	"github.com/sirupsen/logrus"
)

var (
	DefaultClientTimeout     = 10 * time.Second
	DefaultClientTimeToLive  = 24 * time.Hour
	DefaultClientMaxCapacity = 1024
	DefaultClientMaxConns    = 256
)

type ClientOptions struct {
	Logger      logrus.FieldLogger
	Timeout     time.Duration
	TimeToLive  time.Duration
	MaxCapacity int
	MaxConns    int
}

func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Logger:      logrus.New(),
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
	Send(context.Context, net.Addr, message.Message) error
}

type killableConn struct {
	kill     chan<- struct{}
	messages chan<- message.Message
}

type client struct {
	opts ClientOptions

	mu    *sync.Mutex
	conns bound.Map
}

func NewClient(opts ClientOptions) Client {
	client := &client{
		opts: opts,

		mu: new(sync.Mutex),
		conns: bound.NewMapWithRecover(opts.MaxConns, bound.DropLRU(func(k, v interface{}) {
			close(v.(killableConn).kill)
		})),
	}
	return client
}

func (client *client) Send(ctx context.Context, to net.Addr, m message.Message) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	// Get an existing connection, or dial a new connection. Dialing a new
	// connection is non-blocking, and will immediately return a messaging
	// channel that can be used to buffer messages until the connection is
	// successfully dialed.
	address := to.String()
	conn, ok := client.conns.Get(address)
	if !ok {
		kill := make(chan struct{})
		conn = killableConn{
			kill:     kill,
			messages: dial(client.opts, address, client.shutdown, client.errorf, kill),
		}
		client.conns.Set(address, conn)
	}

	// Send the message to the dialed connection.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case conn.(killableConn).messages <- m:
		return nil
	}
}

func (client *client) shutdown(address string) {
	client.mu.Lock()
	defer client.mu.Unlock()

	client.conns.Remove(address)
}

func (client *client) errorf(format string, args ...interface{}) {
	client.opts.Logger.Errorf(format, args)
}

func dial(opts ClientOptions, address string, shutdown func(string), errorf func(string, ...interface{}), kill <-chan struct{}) chan<- message.Message {
	messages := make(chan message.Message, opts.MaxCapacity)

	go func() {
		defer shutdown(address)

		// Dial a new connection. During this time, messages will buffer in the
		// messages channel.
		ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
		defer cancel()

		conn, err := new(net.Dialer).DialContext(ctx, "tcp", address)
		if err != nil {
			errorf("dial connection: %v", err)
			return
		}
		defer func() {
			if err := conn.Close(); err != nil {
				errorf("close connection: %v", err)
			}
		}()

		// Read messages and write them to the connection until the context is
		// cancelled.
		ctx, cancel = context.WithTimeout(context.Background(), opts.TimeToLive)
		defer cancel()

		for {
			select {
			case <-kill:
				errorf("kill connection: %v", ctx.Err())
				return
			case <-ctx.Done():
				errorf("timeout connection: %v", ctx.Err())
				return
			case m := <-messages:
				if err := m.Marshal(conn); err != nil {
					errorf("write to connection: %v", err)
					return
				}
			}
		}
	}()

	return messages
}
