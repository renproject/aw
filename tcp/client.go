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

type ClientOptions struct {
	Logger         logrus.FieldLogger
	Timeout        time.Duration
	MaxConnections int
}

type ClientConns struct {
	options ClientOptions

	connsMu *sync.RWMutex
	conns   map[string]net.Conn
}

func NewClientConns(options ClientOptions) *ClientConns {
	return &ClientConns{
		options: options,

		connsMu: new(sync.RWMutex),
		conns:   map[string]net.Conn{},
	}
}

func (clientConns *ClientConns) Dial(ctx context.Context, addr net.Addr) (net.Conn, error) {
	// FIXME: There is currently no way to close a fault connection. After a
	// connection has faulted more than three times, it should be closed and
	// removed from the connection map. If it is needed in the future, it can be
	// re-dialed.

	// Pre-condition checks
	if addr == nil {
		panic("pre-condition violation: nil net.Addr")
	}
	if addr.Network() != "tcp" {
		panic(fmt.Errorf("pre-condition violation: expected network=tcp, got network=%v", addr.Network()))
	}

	clientConns.connsMu.RLock()
	conn := clientConns.conns[addr.String()]
	clientConns.connsMu.RUnlock()

	if conn != nil {
		return conn, nil
	}

	clientConns.connsMu.Lock()
	defer clientConns.connsMu.Unlock()

	if len(clientConns.conns) >= clientConns.options.MaxConnections {
		return nil, fmt.Errorf("error dialing %v: exceeded max connections", addr.Network())
	}

	conn = clientConns.conns[addr.String()]
	if conn != nil {
		return conn, nil
	}

	conn, err := net.DialTimeout("tcp", addr.String(), clientConns.options.Timeout)
	if err != nil {
		return nil, err
	}
	clientConns.conns[addr.String()] = conn

	return conn, nil
}

func (clientConns *ClientConns) Close(addr net.Addr) error {
	defer func() {
		// Recovery is needed because concurrent closing a net.Conn while
		// writing to it might cause a panic.
		if r := recover(); r != nil {
			clientConns.options.Logger.Errorf("recover while closing tcp connection to %v: %v", addr.String(), r)
		}
	}()

	clientConns.connsMu.Lock()
	defer clientConns.connsMu.Unlock()

	conn := clientConns.conns[addr.String()]
	if conn != nil {
		return nil
	}
	delete(clientConns.conns, addr.String())

	// FIXME: Double check that closing a connection concurrently with another
	// write that it will not panic the goroutine (or cause undefined
	// behaviour). If it does panic (or cause undefined behaviour), then we will
	// either need to abstract over a net.Conn to include a write lock, or
	// recover during closure and writing.
	return conn.Close()
}

type Client struct {
	conns    ClientConns
	messages protocol.MessageReceiver
}

func NewClient(conns ClientConns, messages protocol.MessageReceiver) *Client {
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
	// FIXME: Handle errors without panicking.

	defer func() {
		// Recovery is needed because concurrent closing a net.Conn while
		// writing to it might cause a panic.
		if r := recover(); r != nil {
			client.conns.options.Logger.Errorf("recover while writing to tcp connection to %v: %v", messageWire.To.String(), r)
		}
	}()

	conn, err := client.conns.Dial(ctx, messageWire.To)
	if err != nil {
		panic("unimplemented")
	}

	messageData, err := messageWire.Message.MarshalBinary()
	if err != nil {
		panic("unimplemented")
	}

	n, err := conn.Write(messageData)
	if n != len(messageData) || err != nil {
		if n != len(messageData) {
			client.conns.options.Logger.Errorf("error writing to tcp connection to %v: expected n=%v, got n=%v", messageWire.To.String(), len(messageData), n)
		}
		if err != nil {
			client.conns.options.Logger.Errorf("error writing to tcp connection to %v: %v", messageWire.To.String(), err)
		}

		// TODO: Do not close the connection immediately. Wait for multiple
		// errors to happen in a row.

		if err := client.conns.Close(messageWire.To); err != nil {
			client.conns.options.Logger.Errorf("error closing tcp connection to %v: %v", messageWire.To.String(), err)
		}
	}
}
