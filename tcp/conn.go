package tcp

import (
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/renproject/aw/protocol"
)

type ConnPool interface {
	Send(net.Addr, protocol.Message) error
}

type ConnPoolOptions struct {
	Logger      logrus.FieldLogger
	TimeoutDial time.Duration
	TimeoutConn time.Duration
}

func (options *ConnPoolOptions) setZerosToDefaults() {
	if options.Logger == nil {
		options.Logger = logrus.New()
	}
	if options.TimeoutDial == 0 {
		options.TimeoutDial = time.Second
	}
	if options.TimeoutConn == 0 {
		options.TimeoutConn = time.Minute
	}
}

type connPool struct {
	options ConnPoolOptions
	mu      *sync.Mutex
	conns   map[string]net.Conn
}

func NewConnPool(options ConnPoolOptions) ConnPool {
	options.setZerosToDefaults()
	return &connPool{
		options: options,
		mu:      new(sync.Mutex),
		conns:   map[string]net.Conn{},
	}
}

func (pool *connPool) Send(to net.Addr, m protocol.Message) (err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	conn, ok := pool.conns[to.String()]
	if !ok {
		conn, err = net.DialTimeout(to.Network(), to.String(), pool.options.TimeoutDial)
		if err != nil {
			return err
		}
		pool.conns[to.String()] = conn

		go func() {
			<-time.After(pool.options.TimeoutConn)

			pool.mu.Lock()
			defer pool.mu.Unlock()

			if err := pool.conns[to.String()].Close(); err != nil {
				pool.options.Logger.Errorf("error closing connection to %v: %v", to, err)
			}
			delete(pool.conns, to.String())
		}()
	}

	return m.Write(conn)
}
