package tcp

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
)

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
	Logger         logrus.FieldLogger
	Timeout        time.Duration        // Timeout when dialing new connections.
	TimeToLive     time.Duration        // Time-to-live for connections.
	MaxConnections int                  // Max connections allowed.
	Handshaker     handshake.Handshaker // Handshaker to use while making connections
}

func (options *ConnPoolOptions) setZerosToDefaults() {
	if options.Logger == nil {
		options.Logger = logrus.New()
	}
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
	options ConnPoolOptions
	mu      *sync.Mutex
	conns   map[string]conn
}

type conn struct {
	conn    net.Conn
	session protocol.Session
}

// NewConnPool returns a ConnPool with no existing connections. It is safe for
// concurrent use.
func NewConnPool(options ConnPoolOptions) ConnPool {
	options.setZerosToDefaults()
	return &connPool{
		options: options,
		mu:      new(sync.Mutex),
		conns:   map[string]conn{},
	}
}

func (pool *connPool) Send(to net.Addr, m protocol.Message) (err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	toStr := to.String()
	c, ok := pool.conns[toStr]
	if !ok {
		ctx, cancel := context.WithTimeout(context.Background(), pool.options.Timeout)
		defer cancel()
		netConn, err := net.DialTimeout(to.Network(), toStr, pool.options.Timeout)
		if err != nil {
			return err
		}

		var session protocol.Session
		if pool.options.Handshaker != nil {
			session, err = pool.options.Handshaker.Handshake(ctx, netConn)
			if err != nil {
				return err
			}
		}

		c = conn{
			conn:    netConn,
			session: session,
		}
		pool.conns[toStr] = c

		go func() {
			<-time.After(pool.options.TimeToLive)

			pool.mu.Lock()
			defer pool.mu.Unlock()

			if err := pool.conns[toStr].conn.Close(); err != nil {
				pool.options.Logger.Errorf("error closing connection to %v: %v", to, err)
			}
			delete(pool.conns, toStr)
		}()
	}

	if c.session != nil {
		return c.session.WriteMessage(m, c.conn)
	}
	data, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = c.conn.Write(data)
	return err
}
