package codec

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/renproject/id"
)

type gcmNonce struct {
	// top and bottom together represent the top 32 bits and bottom 64 bits of a 96 bit unsigned integer
	top       uint32
	bottom    uint64
	countDown bool
}

func (nonce gcmNonce) next() {
	if nonce.countDown {
		nonce.pred()
	} else {
		nonce.succ()
	}

}

func (nonce gcmNonce) succ() {
	nonce.bottom += 1
	// If bottom overflows, increment top by 1
	if nonce.bottom == 0 {
		nonce.top += 1
	}
}

func (nonce gcmNonce) pred() {
	nonce.bottom -= 1
	// If bottom underflows, decrement top by 1
	if nonce.bottom == math.MaxUint64 {
		nonce.top -= 1
	}
}

// A GCMSession stores the state of a GCM authenticated/encrypted session. This
// includes the read/write nonces, memory buffers, and the GCM cipher itself.
type GCMSession struct {
	gcm        cipher.AEAD
	readNonce  gcmNonce
	writeNonce gcmNonce
}

// NewGCMSession accepts a symmetric secret key and returns a new GCMSession
// that is configured using the symmetric secret key.
func NewGCMSession(key [32]byte, self, remote id.Signatory) (*GCMSession, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return &GCMSession{}, fmt.Errorf("creating aes cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return &GCMSession{}, fmt.Errorf("creating gcm cipher: %v", err)
	}

	gcmSession := &GCMSession{
		gcm:        gcm,
		readNonce:  gcmNonce{},
		writeNonce: gcmNonce{},
	}

	if bytes.Compare(self[:], remote[:]) < 0 {
		gcmSession.writeNonce.top = math.MaxUint32
		gcmSession.writeNonce.bottom = math.MaxUint64
		gcmSession.writeNonce.countDown = true
	} else {
		gcmSession.readNonce.top = math.MaxUint32
		gcmSession.readNonce.bottom = math.MaxUint64
		gcmSession.readNonce.countDown = true
	}
	return gcmSession, nil
}

// GCMEncoder accepts a GCMSession and an encoder that wraps data encryption
func GCMEncoder(session *GCMSession, enc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		nonceBuf := [12]byte{}
		binary.BigEndian.PutUint32(nonceBuf[:4], session.writeNonce.top)
		binary.BigEndian.PutUint64(nonceBuf[4:], session.writeNonce.bottom)
		session.writeNonce.next()
		encoded := session.gcm.Seal(nil, nonceBuf[:], buf, nil)
		_, err := enc(w, encoded)
		if err != nil {
			return 0, fmt.Errorf("encoding sealed data: %v", err)
		}
		return len(buf), nil
	}
}

// GCMDEcoder accepts a GCMSession and a decoder that wraps data decryption
func GCMDecoder(session *GCMSession, dec Decoder) Decoder {
	return func(r io.Reader, buf []byte) (int, error) {
		buf = buf[:len(buf)+16]
		n, err := dec(r, buf)
		if err != nil {
			return n, fmt.Errorf("decoding data: %v", err)
		}
		nonceBuf := [12]byte{}
		binary.BigEndian.PutUint32(nonceBuf[:4], session.readNonce.top)
		binary.BigEndian.PutUint64(nonceBuf[4:], session.readNonce.bottom)
		session.readNonce.next()
		decrypted, err := session.gcm.Open(nil, nonceBuf[:], buf[:n], nil)

		if err != nil {
			return 0, fmt.Errorf("opening sealed data: %v", err)
		}
		copy(buf, decrypted)

		return len(decrypted), nil
	}
}
