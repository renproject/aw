package channel

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/id"
)

type ChannelPool struct {
	chsMu *sync.Mutex
	chs   map[id.Signatory]Channel
}

func NewPool() *ChannelPool {
	return &ChannelPool{
		chsMu: new(sync.Mutex),
		chs:   map[id.Signatory]Channel{},
	}
}

func (pool *ChannelPool) Close(peer id.Signatory) error {
	pool.chsMu.Lock()
	defer pool.chsMu.Unlock()

	if ch, ok := pool.chs[peer]; ok {
		delete(pool.chs, peer)
		if err := ch.Connection().Close(); err != nil {
			return fmt.Errorf("close channel to %v: %v", peer, err)
		}
		return nil
	}
	return nil
}

func (pool *ChannelPool) CloseAll() error {
	pool.chsMu.Lock()
	defer pool.chsMu.Unlock()

	errs := []string{}

	for peer, ch := range pool.chs {
		if err := ch.Connection().Close(); err != nil {
			errs = append(errs, fmt.Sprintf("close channel to %v: %v", peer, err))
		}
	}
	pool.chs = map[id.Signatory]Channel{}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("close channels: %v", strings.Join(errs, ", "))
}

func (pool *ChannelPool) HighestPeerWinsHandshake(self id.Signatory, h handshake.Handshake) handshake.Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		enc, dec, remote, err := h(conn, enc, dec)
		if err != nil {
			return enc, dec, remote, err
		}

		if bytes.Compare(self[:], remote[:]) < 0 {
			keepAlive := [1]byte{}
			if _, err := dec(conn, keepAlive[:]); err != nil {
				return enc, dec, remote, fmt.Errorf("decoding keep-alive: %v", err)
			}
			if keepAlive[0] == 0 {
				return nil, nil, remote, fmt.Errorf("remote peer killed connection")
			}
		} else {
			if _, err := enc(conn, []byte{0x01}); err != nil {
				return enc, dec, remote, fmt.Errorf("encoding keep-alive: %v", err)
			}
		}

		pool.chsMu.Lock()
		defer pool.chsMu.Unlock()

		if existingCh, ok := pool.chs[remote]; ok {
			// Ignore the error.
			_ = existingCh.Connection().Close()
		}
		pool.chs[remote] = Channel{
			self:    self,
			remote:  remote,
			conn:    conn,
			encoder: enc,
			decoder: dec,
		}
		return enc, dec, remote, nil
	}
}
