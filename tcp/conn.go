package tcp

import (
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/protocol"
)

type ConnPool interface {
	Write(net.Addr, protocol.Message, time.Duration) error
}

type connPool struct {
	mu    *sync.Mutex
	conns map[string]net.Conn
}

func NewConnPool() ConnPool {
	return &connPool{
		mu:    new(sync.Mutex),
		conns: map[string]net.Conn{},
	}
}

func (pool *connPool) Write(to net.Addr, m protocol.Message, timeout time.Duration) (err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	conn, ok := pool.conns[to.String()]
	if !ok {
		conn, err = net.DialTimeout(to.Network(), to.String(), timeout) // TODO: Make this value configurable.
		if err != nil {
			return err
		}
		pool.conns[to.String()] = conn
	}

	return m.Write(conn)
}
