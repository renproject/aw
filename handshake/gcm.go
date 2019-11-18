package handshake

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"

	"github.com/renproject/aw/protocol"
)

type gcmSessionManager struct{}

func NewGCMSessionManager() protocol.SessionManager {
	return gcmSessionManager{}
}

func (gcmSessionManager) NewSession(peerID protocol.PeerID, key []byte) protocol.Session {
	key32 := [32]byte{}
	copy(key32[:], key)
	return NewGCMSession(peerID, key32)
}

func (gcmSessionManager) NewSessionKey() []byte {
	key := [32]byte{}
	n, err := rand.Read(key[:])
	if n != 32 {
		panic(fmt.Errorf("invariant violation: cannot generate session key: expected n=32, got n=%v", n))
	}
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot generate session key: %v", err))
	}
	return key[:]
}

type gcmSession struct {
	peerID protocol.PeerID
	key    [32]byte
	gcm    cipher.AEAD
	rand   *rand.Rand
}

func NewGCMSession(peerID protocol.PeerID, key [32]byte) protocol.Session {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot create cipher: %v", err))
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot create galios/counter mode: %v", err))
	}
	seed := binary.BigEndian.Uint64(key[:8])
	return &gcmSession{
		peerID: peerID,
		key:    key,
		gcm:    gcm,
		rand:   rand.New(rand.NewSource(int64(seed))),
	}
}

func (session *gcmSession) ReadMessageOnTheWire(r io.Reader) (protocol.MessageOnTheWire, error) {
	otw := protocol.MessageOnTheWire{}
	otw.From = session.peerID
	if err := otw.Message.UnmarshalReader(r); err != nil {
		return otw, err
	}

	nonce := make([]byte, session.gcm.NonceSize())
	_, err := session.rand.Read(nonce)
	if err != nil {
		return otw, err
	}
	otw.Message.Body, err = session.gcm.Open(nil, nonce, otw.Message.Body, nil)
	if err != nil {
		return otw, err
	}
	length := 8
	if otw.Message.Variant == protocol.Multicast || otw.Message.Variant == protocol.Broadcast {
		length = 40
	}
	otw.Message.Length = protocol.MessageLength(len(otw.Message.Body) + length)
	return otw, nil
}

func (session *gcmSession) WriteMessage(w io.Writer, message protocol.Message) error {
	nonce := make([]byte, session.gcm.NonceSize())
	_, err := session.rand.Read(nonce)
	if err != nil {
		return err
	}
	message.Body = session.gcm.Seal(nil, nonce, message.Body, nil)
	length := 8
	if message.Variant == protocol.Multicast || message.Variant == protocol.Broadcast {
		length = 40
	}
	message.Length = protocol.MessageLength(len(message.Body) + length)

	data, err := message.MarshalBinary()
	if err != nil {
		return fmt.Errorf("error writing message: %v", err)
	}
	n, err := w.Write(data)
	if n != len(data) {
		return fmt.Errorf("error writing message: expected n=%v, got n=%v", len(data), n)
	}
	if err != nil {
		return fmt.Errorf("error writing message: %v", err)
	}
	return nil
}
