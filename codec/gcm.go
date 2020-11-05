package codec

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
)

// A GCMSession stores the state of a GCM authenticated/encrypted session. This
// includes the read/write nonces, memory buffers, and the GCM cipher itself.
type GCMSession struct {
	gcm   cipher.AEAD
	rRand *rand.Rand
	wRand *rand.Rand
	wBuf  []byte
}

// NewGCMSession accepts a symmetric secret key and returns a new GCMSession
// that is configured using the symmetric secret key.
func NewGCMSession(key [32]byte) (*GCMSession, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return &GCMSession{}, fmt.Errorf("creating aes cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return &GCMSession{}, fmt.Errorf("creating gcm cipher: %v", err)
	}
	return &GCMSession{
		gcm:   gcm,
		rRand: rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(key[:8])))),
		wRand: rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(key[:8])))),
		wBuf:  make([]byte, gcm.NonceSize()),
	}, nil
}

func GCMEncoder(session *GCMSession, enc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		_, err := session.wRand.Read(session.wBuf)
		if err != nil {
			return 0, fmt.Errorf("generating randomness: %v", err)
		}
		n, err := enc(w, append(session.wBuf, session.gcm.Seal(nil, session.wBuf, buf, nil)...))
		if err != nil {
			return n, fmt.Errorf("encoding sealed data: %v", err)
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
		decrypted, err := session.gcm.Open(nil, buf[:12], buf[12:n], nil)
		if err != nil {
			return 0, fmt.Errorf("opening sealed data: %v", err)
		}
		copy(buf, decrypted)

		return n, nil
	}
}
