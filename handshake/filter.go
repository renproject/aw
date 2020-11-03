package handshake

import (
	"fmt"
	"net"

	"github.com/renproject/aw/codec"
	"github.com/renproject/id"
)

// Filter accepts a filtering function and a Handshake function, and returns
// wrapping Handshake function that runs the wrapped Handshake before applying
// the filtering function to the remote peer ID. If the wrapped Handshake
// returns an error, the filtering function will be skipped, and the error will
// be returned. Otherwise, the filtering function will be called and its error
// returned.
func Filter(f func(id.Signatory) error, h Handshake) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		enc, dec, remote, err := h(conn, enc, dec)
		if err != nil {
			return enc, dec, remote, err
		}
		if err := f(remote); err != nil {
			return enc, dec, remote, fmt.Errorf("filter %v: %v", remote, err)
		}
		return enc, dec, remote, nil
	}
}
