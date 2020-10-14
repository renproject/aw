package channel

import (
	"bytes"
	"fmt"
	"net"
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

func (pool *ChannelPool) HighestPeerWinsHandshake(self id.Signatory, h handshake.Handshake) handshake.Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		enc, dec, remote, err := h(conn, enc, dec)
		if err != nil {
			return enc, dec, remote, err
		}

		pool.chsMu.Lock()
		defer pool.chsMu.Unlock()

		if bytes.Compare(self[:], remote[:]) < 0 {
			keepAlive := [1]byte{}
			if _, err := dec(conn, keepAlive[:]); err != nil {
				return enc, dec, remote, err
			}
			if keepAlive[0] == 0 {
				return nil, nil, remote, fmt.Errorf("remote peer killed connection")
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

		if _, ok := pool.chs[remote]; ok {
			enc(conn, []byte{0x00})
			return nil, nil, remote, fmt.Errorf("kill connection")
		}

		pool.chs[remote] = Channel{
			self:    self,
			remote:  remote,
			conn:    conn,
			encoder: enc,
			decoder: dec,
		}
		enc(conn, []byte{0x01})
		return enc, dec, remote, nil
	}
}
