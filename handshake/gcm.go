package handshake

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"math/rand"

	"github.com/renproject/id"
)

type GCMSession struct {
	gcm         cipher.AEAD
	nonceRand   *rand.Rand
	nonceBuffer []byte
	other       id.Signatory
}

func NewGCMSession(key []byte, other id.Signatory) (Session, error) {
	if len(key) < 8 {
		return GCMSession{}, fmt.Errorf("expected key length>=8, got key length=%v", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return GCMSession{}, fmt.Errorf("creating aes cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return GCMSession{}, fmt.Errorf("creating gcm wrapped cipher: %v", err)
	}

	return GCMSession{
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

func (session GCMSession) Signatory() id.Signatory {
	return session.other
}
