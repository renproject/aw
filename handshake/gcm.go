package handshake

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
)

type GCMSession struct {
	inner io.ReadWriter

	gcm         cipher.AEAD
	nonceRand   *rand.Rand
	nonceBuffer []byte
}

func NewGCMSession(inner io.ReadWriter, key []byte, maxSize int) (Session, error) {
	if len(key) < 8 {
		return GCMSession{}, fmt.Errorf("key is too small: expected>8, got=%v", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return GCMSession{}, fmt.Errorf("creating new key cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return GCMSession{}, fmt.Errorf("creating new gcm aead: %v", err)
	}

	return GCMSession{
		inner: inner,

		gcm:         gcm,
		nonceRand:   rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(key[:8])))),
		nonceBuffer: make([]byte, gcm.NonceSize()),
	}, nil
}

func (session GCMSession) Encrypt(p []byte) ([]byte, error) {
	_, err := session.nonceRand.Read(session.nonceBuffer)
	if err != nil {
		return nil, err
	}

	return session.gcm.Seal(nil, session.nonceBuffer, p, nil), nil
}

func (session GCMSession) Decrypt(p []byte) ([]byte, error) {
	if _, err := session.nonceRand.Read(session.nonceBuffer); err != nil {
		return nil, err
	}

	return session.gcm.Open(nil, session.nonceBuffer, p, nil)
}
