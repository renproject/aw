package tcp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

// ClientOptions define how the Client manages its connections with remote
// servers. It also defines some other simple behaviours, such as logging.
type ClientOptions struct {
	// Logger is used to log information and errors.
	Logger logrus.FieldLogger
	// Timeout after which the Client will stop an attempt to dial a remote
	// server.
	Timeout time.Duration
	// MaxConnections to remote servers that the Client will maintain.
	MaxConnections int
}

type ClientConn struct {
	mu   *sync.Mutex
	conn net.Conn
}

func NewClientConn() *ClientConn {
	return &ClientConn{
		mu:   new(sync.Mutex),
		conn: nil,
	}
}

// ClientConns is an in memory cache of connections to remote servers that is
// safe for concurrent use.
type ClientConns struct {
	options ClientOptions
	connsMu *sync.RWMutex
	conns   map[string]*ClientConn
}

// NewClientConns returns an empty ClientConns that will use ClientOptions to
// control how to dials remote servers.
func NewClientConns(options ClientOptions) *ClientConns {
	return &ClientConns{
		options: options,
		connsMu: new(sync.RWMutex),
		conns:   map[string]*ClientConn{},
	}
}

// Dial a remote server. If a connection to the remote server already exists,
// then that connection is immediately returned. If a connection to the remote
// server does not exist, then one is established.
func (clientConns *ClientConns) Dial(ctx context.Context, addr net.Addr) (net.Conn, error) {
	// Pre-condition checks
	if addr == nil {
		panic("pre-condition violation: nil net.Addr")
	}
	if addr.Network() != "tcp" {
		panic(fmt.Errorf("pre-condition violation: expected network=tcp, got network=%v", addr.Network()))
	}

	// Check for an existing connection
	clientConns.connsMu.RLock()
	conn := clientConns.conns[addr.String()]
	clientConns.connsMu.RUnlock()
	if conn != nil && conn.conn != nil {
		return conn.conn, nil
	}

	// Protect the cache from concurrent writes and establish a connection that
	// can be dialed
	conn, err := func() (*ClientConn, error) {
		clientConns.connsMu.Lock()
		defer clientConns.connsMu.Unlock()

		// Double-check the connection, because while waiting to acquire the
		// write lock another goroutine may have already established the
		// connection
		conn = clientConns.conns[addr.String()]
		if conn != nil {
			return conn, nil
		}

		// Return an error if we are already maintaining the maximum number of
		// connections.
		if len(clientConns.conns) >= clientConns.options.MaxConnections {
			return nil, fmt.Errorf("error dialing %v: exceeded max connections", addr.Network())
		}

		clientConns.conns[addr.String()] = NewClientConn()
		return clientConns.conns[addr.String()], nil
	}()
	if err != nil {
		return nil, err
	}
	if conn.conn != nil {
		return conn.conn, nil
	}

	// A new connection needs to be dialed, so we lock the connection to prevent
	// multiple dials against the same remote server
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// Double-check the connection, because while waiting to acquire the write lock
	// another goroutine may have already dialed the remote server
	if conn.conn != nil {
		return conn.conn, nil
	}

	conn.conn, err = net.DialTimeout("tcp", addr.String(), clientConns.options.Timeout)
	return conn.conn, err
}

func (clientConns *ClientConns) Close(addr net.Addr) error {
	// Protect the cache from concurrent writes and delete the connection
	// associated with this address
	conn := func() *ClientConn {
		clientConns.connsMu.Lock()
		defer clientConns.connsMu.Unlock()

		conn := clientConns.conns[addr.String()]
		delete(clientConns.conns, addr.String())

		return conn
	}()
	// If the connection is nil, or has not been dialed, then there is nothing
	// else to do
	if conn == nil || conn.conn == nil {
		return nil
	}

	// Otherwise, protect the connection from concurrent writes and free the
	// underlying network connection that has been dialed
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// Double-check
	if conn == nil || conn.conn == nil {
		return nil
	}
	err := conn.conn.Close()
	conn.conn = nil

	return err
}

type Client struct {
	conns    *ClientConns
	messages protocol.MessageReceiver
}

func NewClient(conns *ClientConns, messages protocol.MessageReceiver) *Client {
	return &Client{
		conns:    conns,
		messages: messages,
	}
}

func (client *Client) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageWire := <-client.messages:
			client.sendMessageOnTheWire(ctx, messageWire)
		}
	}
}

func (client *Client) sendMessageOnTheWire(ctx context.Context, messageWire protocol.MessageOnTheWire) {
	conn, err := client.conns.Dial(ctx, messageWire.To)
	if err != nil {
		client.conns.options.Logger.Errorf("error dialing tcp connection to %v: %v", messageWire.To.String(), err)
		return
	}

	if err := messageWire.Message.Write(conn); err != nil {
		client.conns.options.Logger.Errorf("error writing to tcp connection to %v: %v", messageWire.To.String(), err)
		return
	}
}
