package tcp

import (
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/protocol"
)

type ConnPool interface {
	Write(to *net.TCPAddr, m protocol.Message) error
}

type connPool struct {
	mu    *sync.Mutex
	conns map[string]*net.TCPConn
}

func NewConnPool() ConnPool {
	return &connPool{
		mu:    new(sync.Mutex),
		conns: map[string]*net.TCPConn{},
	}
}

func (pool *connPool) Write(to *net.TCPAddr, m protocol.Message) (err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	conn, ok := pool.conns[to.String()]
	if !ok {
		conn, err = net.DialTCP("tcp", nil, to)
		if err != nil {
			return err
		}
		conn.SetKeepAlive(true)
		conn.SetKeepAlivePeriod(10 * time.Second) // TODO: Make this value configurable.
		pool.conns[to.String()] = conn
	}

	return m.Write(conn)
}
