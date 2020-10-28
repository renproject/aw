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

// Handshake functions accept a connection, an encoder, and decoder. The encoder
// and decoder are used to establish an authenticated and encrypted connection.
// A new encoder and decoder are returned, which wrap the accpted encoder and
// decoder with any additional functionality required to perform authentication
// and encryption. The identity of the remote peer is also returned.
type Handshake func(net.Conn, codec.Encoder, codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error)

// InsecureHandshake returns a Handshake that does no authentication or
// encryption. During the handshake, the local peer writes its own identity to
// the connection, and then reads the identity of the remote peer. No
// verification of identities is done. InsecureHandshake should only be used in
// private networks.
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
