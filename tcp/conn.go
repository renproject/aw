package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

// ErrTooManyConnections is returned the current number of connections exceeds the limit.
var ErrTooManyConnections = errors.New("too many connections")

// A ConnPool maintains multiple connections to different remote peers and
// re-uses these connections when sending multiple message to the peer. If a
// connection to a peer does not exist when a message is sent, then it is
// established. When there are multiple Clients, they should all use a shared
// ConnPool, and therefore all implementations must be safe for concurrent use.
type ConnPool interface {
	Send(net.Addr, protocol.Message) error
}

// ConnPoolOptions are used to parameterise the behaviour of a ConnPool.
type ConnPoolOptions struct {
	Timeout        time.Duration // Timeout when dialing new connections.
	TimeToLive     time.Duration // Time-to-live for connections.
	MaxConnections int           // Max connections allowed.
}

func (options *ConnPoolOptions) setZerosToDefaults() {
	if options.Timeout == 0 {
		options.Timeout = 5 * time.Second
	}
	if options.TimeToLive == 0 {
		options.TimeToLive = 5 * time.Minute
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = 512
	}
}

type connPool struct {
	logger     logrus.Logger
	options    ConnPoolOptions
	handshaker handshake.Handshaker // Handshaker to use while making connections

	mu    *sync.RWMutex
	conns map[string]conn
}

type conn struct {
	conn    net.Conn
	session protocol.Session
}

// NewConnPool returns a ConnPool with no existing connections. It is safe for
// concurrent use.
func NewConnPool(options ConnPoolOptions, logger logrus.FieldLogger, handshaker handshake.Handshaker) ConnPool {
	if logger == nil {
		logger = logrus.New()
	}
	options.setZerosToDefaults()
	if handshaker == nil {
		panic("ConnPool cannot have a nil handshaker")
	}
	return &connPool{
		mu:    new(sync.RWMutex),
		conns: map[string]conn{},

		options:    options,
		handshaker: handshaker,
	}
}

func (pool *connPool) Send(to net.Addr, m protocol.Message) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	toStr := to.String()
	c, ok := pool.conns[toStr]
	if !ok {
		if len(pool.conns) >= pool.options.MaxConnections {
			return ErrTooManyConnections
		}

		var err error
		c, err = pool.connect(to)
		if err != nil {
			return err
		}

		pool.conns[toStr] = c
		go pool.closeConn(toStr)
	}

	if err := c.session.WriteMessage(c.conn, m); err != nil {
		pool.logger.Errorf("error in session: %v, closing connection...", err)
		pool.closeConnImmediately(toStr)
	}
	return nil
}

func (pool *connPool) connect(to net.Addr) (conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pool.options.Timeout)
	defer cancel()

	netConn, err := net.DialTimeout(to.Network(), to.String(), pool.options.Timeout)
	if err != nil {
		return conn{}, err
	}

	// Set a timeout for the handshake process
	deadline := time.Now().Add(pool.options.Timeout)
	if err := netConn.SetDeadline(deadline); err != nil {
		return conn{}, err
	}

	session, err := pool.handshaker.Handshake(ctx, netConn)
	if err != nil {
		return conn{}, err
	}
	if session == nil {
		return conn{}, fmt.Errorf("nil session [addr = %v] returned by handshaker", to)
	}

	// Reset the timeout back
	if err := netConn.SetDeadline(time.Time{}); err != nil {
		return conn{}, err
	}

	return conn{
		conn:    netConn,
		session: session,
	}, nil
}

func (pool *connPool) closeConn(to string) {
	<-time.After(pool.options.TimeToLive)
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if err := pool.conns[to].conn.Close(); err != nil {
		pool.logger.Errorf("error closing connection to %v: %v", to, err)
	}
	delete(pool.conns, to)
}

func (pool *connPool) closeConnImmediately(to string) {
	if err := pool.conns[to].conn.Close(); err != nil {
		pool.logger.Errorf("error closing connection to %v: %v", to, err)
	}
	delete(pool.conns, to)
}
