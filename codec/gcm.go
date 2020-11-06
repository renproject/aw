package codec

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"math"

	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type gcmNonce struct {
	top       uint32
	bottom    uint64
	countDown bool
}

func (nonce *gcmNonce) change() {
	if nonce.countDown {
		nonce.pred()
	} else {
		nonce.succ()
	}

}

func (nonce *gcmNonce) succ() {
	nonce.bottom += 1
	if nonce.bottom == 0 {
		nonce.top += 1
	}
}

func (nonce *gcmNonce) pred() {
	nonce.bottom -= 1
	if nonce.bottom == math.MaxUint64 {
		nonce.top -= 1
	}
}

// A GCMSession stores the state of a GCM authenticated/encrypted session. This

// includes the read/write nonces, memory buffers, and the GCM cipher itself.
type GCMSession struct {
	gcm        cipher.AEAD
	readNonce  *gcmNonce
	writeNonce *gcmNonce
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
		readNonce:  &gcmNonce{},
		writeNonce: &gcmNonce{},
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

func GCMEncoder(session *GCMSession, enc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		nonceBufTop, err := surge.ToBinary(session.writeNonce.top)
		if err != nil {
			return 0, fmt.Errorf("marshaling nonce to bytes: %v", err)
		}
		nonceBufBottom, err := surge.ToBinary(session.writeNonce.bottom)
		if err != nil {
			return 0, fmt.Errorf("marshaling nonce to bytes: %v", err)
		}
		nonceBuf := append(nonceBufTop, nonceBufBottom...)
		session.writeNonce.change()
		encoded := session.gcm.Seal(nil, nonceBuf, buf, nil)
		n, err := enc(w, encoded)
		if err != nil {
			return n, fmt.Errorf("Encoded sealed data: %v", err)
		}
		return n, nil
	}
}

func GCMDecoder(session *GCMSession, dec Decoder) Decoder {
	return func(r io.Reader, buf []byte) (int, error) {
		n, err := dec(r, buf)
		if err != nil {
			return n, fmt.Errorf("decoding data: %v", err)
		}
		nonceBufTop, err := surge.ToBinary(session.readNonce.top)
		if err != nil {
			return 0, fmt.Errorf("marshaling nonce to bytes: %v", err)
		}
		nonceBufBottom, err := surge.ToBinary(session.readNonce.bottom)
		if err != nil {
			return 0, fmt.Errorf("marshaling nonce to bytes: %v", err)
		}
		nonceBuf := append(nonceBufTop, nonceBufBottom...)
		session.readNonce.change()
		decrypted, err := session.gcm.Open(nil, nonceBuf, buf[:n], nil)

		if err != nil {
			return 0, fmt.Errorf("opening sealed data: %v", err)
		}
		copy(buf, decrypted)

		return n, nil
	}
}
