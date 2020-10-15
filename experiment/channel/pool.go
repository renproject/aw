package channel

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/id"
)

var (
	DefaultMinimumExpiryAge = time.Minute
)

type PoolOptions struct {
	MinimumExpiryAge time.Duration
}

func DefaultPoolOptions() PoolOptions {
	return PoolOptions{
		MinimumExpiryAge: DefaultMinimumExpiryAge,
	}
}

func (opts PoolOptions) WithMinimumExpiryAge(minExpiryAge time.Duration) PoolOptions {
	opts.MinimumExpiryAge = minExpiryAge
	return opts
}

type Pool struct {
	opts PoolOptions

	chsMu *sync.Mutex
	chs   map[id.Signatory]Channel
}

func NewPool(opts PoolOptions) *Pool {
	return &Pool{
		opts: opts,

		chsMu: new(sync.Mutex),
		chs:   map[id.Signatory]Channel{},
	}
}

func (pool *Pool) Close(peer id.Signatory) error {
	pool.chsMu.Lock()
	defer pool.chsMu.Unlock()

	if ch, ok := pool.chs[peer]; ok {
		delete(pool.chs, peer)
		if err := ch.Close(); err != nil {
			return fmt.Errorf("close channel to %v: %v", peer, err)
		}
		return nil
	}
	return nil
}

func (pool *Pool) CloseAll() error {
	pool.chsMu.Lock()
	defer pool.chsMu.Unlock()

	errs := []string{}

	for peer, ch := range pool.chs {
		if err := ch.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("close channel to %v: %v", peer, err))
		}
	}
	pool.chs = map[id.Signatory]Channel{}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("close channels: %v", strings.Join(errs, ", "))
}

func (pool *Pool) HighestPeerWinsHandshake(self id.Signatory, h handshake.Handshake) (handshake.Handshake, func(context.Context, id.Signatory)) {
	waitForClose := func(ctx context.Context, remote id.Signatory) {
		pool.chsMu.Lock()
		ch, ok := pool.chs[remote]
		pool.chsMu.Unlock()

		if ok {
			select {
			case <-ctx.Done():
			case <-ch.Done():
				// If the channel is replaced while we are waiting here, we will
				// block forever if the Channel.Close method is not called
				// during replacement. This implies that it is critical that
				// Channel.Close is called whenever a Channel is no longer
				// needed (e.g. it is being replaced).
			}
		}
	}
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		enc, dec, remote, err := h(conn, enc, dec)
		if err != nil {
			return enc, dec, remote, err
		}

		if bytes.Compare(self[:], remote[:]) < 0 {
			keepAlive := [1024]byte{}
			if _, err := dec(conn, keepAlive[:]); err != nil {
				return enc, dec, remote, fmt.Errorf("decoding keep-alive message: %v", err)
			}
			if keepAlive[0] != 0x00 {
				return nil, nil, remote, fmt.Errorf("kill connection from %v", remote)
			}

			pool.chsMu.Lock()
			defer pool.chsMu.Unlock()

			if existingCh, ok := pool.chs[remote]; ok {
				// Ignore the error, because we no longer need this connection.
				_ = existingCh.Close()
			}
			ctx, cancel := context.WithCancel(context.Background())
			pool.chs[remote] = Channel{
				ctx:         ctx,
				cancel:      cancel,
				connectedAt: time.Now(),
				self:        self,
				remote:      remote,
				conn:        conn,
				encoder:     enc,
				decoder:     dec,
			}
			return enc, dec, remote, nil
		}

		// Lock, perform non-blocking operations, and then unblock. This allows
		// us to avoid doing blocking operations (such as encoding/decoding
		// keep-alive messages) while holding the mutex lock.
		pool.chsMu.Lock()
		existingCh, existingChIsOk := pool.chs[remote]
		existingChNeedsReplacement := !existingChIsOk || time.Now().Sub(existingCh.ConnectedAt()) > pool.opts.MinimumExpiryAge
		if existingChNeedsReplacement {
			ctx, cancel := context.WithCancel(context.Background())
			pool.chs[remote] = Channel{
				ctx:         ctx,
				cancel:      cancel,
				connectedAt: time.Now(),
				self:        self,
				remote:      remote,
				conn:        conn,
				encoder:     enc,
				decoder:     dec,
			}
		}
		pool.chsMu.Unlock()

		if !existingChNeedsReplacement {
			_, err := enc(conn, msgKeepAliveFalse)
			// Ignore the error, because we no longer need this connection.
			_ = conn.Close()
			if err != nil {
				return enc, dec, remote, fmt.Errorf("encoding keep-alive message 0x00 to %v: %v", remote, err)
			}
			return enc, dec, remote, fmt.Errorf("kill connection to %v", remote)
		}

		if existingChIsOk {
			// Ignore the error, because we no longer need this connection.
			_ = existingCh.Close()
		}
		if _, err := enc(conn, msgKeepAliveTrue); err != nil {
			// An error occurred while writing the "keep alive" message to
			// the remote peer. This results in an inconsistent state, so we
			// should recover the state by deleting the recently inserted
			// channel. This will cause the remote peer to eventually error
			// on their connection, and will cause our local peer to
			// eventually re-attempt channel creation.
			pool.chsMu.Lock()
			// Ignore the error, because we no longer need this connection.
			_ = pool.chs[remote].Close()
			delete(pool.chs, remote)
			pool.chsMu.Unlock()

			return enc, dec, remote, fmt.Errorf("encoding keep-alive message 0x01 to %v: %v", remote, err)
		}
		return enc, dec, remote, nil
	}, waitForClose
}

var (
	msgKeepAliveFalse = []byte{0x00}
	msgKeepAliveTrue  = []byte{0x01}
)
