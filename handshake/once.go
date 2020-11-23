package handshake

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/codec"
	"github.com/renproject/id"
)

var DefaultMinimumExpiryAge = time.Minute

type OncePoolOptions struct {
	MinimumExpiryAge time.Duration
}

func DefaultOncePoolOptions() OncePoolOptions {
	return OncePoolOptions{
		MinimumExpiryAge: DefaultMinimumExpiryAge,
	}
}

func (opts OncePoolOptions) WithMinimumExpiryAge(minExpiryAge time.Duration) OncePoolOptions {
	opts.MinimumExpiryAge = minExpiryAge
	return opts
}

type onceConn struct {
	timestamp time.Time
	conn      net.Conn
}

type OncePool struct {
	opts OncePoolOptions

	connsMu *sync.Mutex
	conns   map[id.Signatory]onceConn
}

func NewOncePool(opts OncePoolOptions) OncePool {
	return OncePool{
		opts: opts,

		connsMu: new(sync.Mutex),
		conns:   map[id.Signatory]onceConn{},
	}
}

func Once(self id.Signatory, pool *OncePool, h Handshake) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		enc, dec, remote, err := h(conn, enc, dec)
		if err != nil {
			return enc, dec, remote, err
		}

		if bytes.Compare(self[:], remote[:]) < 0 {
			keepAlive := [17]byte{}
			if _, err := dec(conn, keepAlive[:1]); err != nil {
				return enc, dec, remote, fmt.Errorf("decoding keep-alive message: %v", err)
			}
			if keepAlive[0] == 0x00 {
				return nil, nil, remote, fmt.Errorf("kill connection from %v", remote)
			}

			pool.connsMu.Lock()
			defer pool.connsMu.Unlock()

			if existingConn, ok := pool.conns[remote]; ok {
				// Ignore the error, because we no longer need this connection.
				_ = existingConn.conn.Close()
			}
			pool.conns[remote] = onceConn{timestamp: time.Now(), conn: conn}
			return enc, dec, remote, nil
		}

		// Lock, perform non-blocking operations, and then unblock. This allows
		// us to avoid doing blocking operations (such as encoding/decoding
		// keep-alive messages) while holding the mutex lock.
		pool.connsMu.Lock()
		existingConn, existingConnIsOk := pool.conns[remote]
		existingConnNeedsReplacement := !existingConnIsOk || time.Now().Sub(existingConn.timestamp) > pool.opts.MinimumExpiryAge
		if existingConnNeedsReplacement {
			pool.conns[remote] = onceConn{timestamp: time.Now(), conn: conn}
		}
		pool.connsMu.Unlock()

		if !existingConnNeedsReplacement {
			_, err := enc(conn, msgKeepAliveFalse)
			// Ignore the error, because we no longer need this connection.
			_ = conn.Close()
			if err != nil {
				return enc, dec, remote, fmt.Errorf("encoding keep-alive message 0x00 to %v: %v", remote, err)
			}
			return enc, dec, remote, fmt.Errorf("kill connection to %v", remote)
		}

		if existingConnIsOk {
			// Ignore the error, because we no longer need this connection.
			_ = existingConn.conn.Close()
		}
		if _, err := enc(conn, msgKeepAliveTrue); err != nil {
			// An error occurred while writing the "keep alive" message to
			// the remote peer. This results in an inconsistent state, so we
			// should recover the state by deleting the recently inserted
			// channel. This will cause the remote peer to eventually error
			// on their connection, and will cause our local peer to
			// eventually re-attempt channel creation.
			pool.connsMu.Lock()
			// Ignore the error, because we no longer need this connection.
			_ = pool.conns[remote].conn.Close()
			delete(pool.conns, remote)
			pool.connsMu.Unlock()
			return enc, dec, remote, fmt.Errorf("encoding keep-alive message 0x01 to %v: %v", remote, err)
		}

		return enc, dec, remote, nil
	}
}

var (
	msgKeepAliveFalse = []byte{0x00}
	msgKeepAliveTrue  = []byte{0x01}
)
