package handshake

import (
	"fmt"
	"net"

	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/id"
)

const keySize = 32

const encryptionHeaderSize = 113

// encryptedKeySize specifiec the size in bytes of a single key encrypted using ECIES
const encryptedKeySize = encryptionHeaderSize + keySize

type Handshake func(net.Conn, codec.Encoder, codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error)

// InsecureHandshake returns a Handshake that does no negotiation with the
// remote peer. It never returns an error, and always returns the encoder and
// decoder unmodified.
func InsecureHandshake(self id.Signatory) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		if _, err := enc(conn, self[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("encoding local id: %v", err)
		}
		remote := id.Signatory{}
		if _, err := dec(conn, remote[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("decoding remote id: %v", err)
		}
		return enc, dec, remote, nil
	}
}
