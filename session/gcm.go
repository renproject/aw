package session

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
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
	nonce.bottom++
	// If bottom overflows, increment top by 1
	if nonce.bottom == 0 {
		nonce.top++
	}
}

func (nonce gcmNonce) pred() {
	nonce.bottom--
	// If bottom underflows, decrement top by 1
	if nonce.bottom == math.MaxUint64 {
		nonce.top--
	}
}

// A GCMSession stores the state of a GCM authenticated/encrypted session. This
// includes the read/write nonces, memory buffers, and the GCM cipher itself.
type GCMSession struct {
	GCM        cipher.AEAD
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
		GCM:        gcm,
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
func (session *GCMSession) GetReadNonceAndIncrement() [12]byte {
	nonceBuf := [12]byte{}
	binary.BigEndian.PutUint32(nonceBuf[:4], session.readNonce.top)
	binary.BigEndian.PutUint64(nonceBuf[4:], session.readNonce.bottom)
	session.readNonce.next()

	return nonceBuf
}

func (session *GCMSession) GetWriteNonceAndIncrement() [12]byte {
	nonceBuf := [12]byte{}
	binary.BigEndian.PutUint32(nonceBuf[:4], session.writeNonce.top)
	binary.BigEndian.PutUint64(nonceBuf[4:], session.writeNonce.bottom)
	session.writeNonce.next()

	return nonceBuf
}
