package session

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"

	"github.com/renproject/aw/protocol"
)

type gcmSessionCreator struct{}

func NewGCMSessionCreator() protocol.SessionCreator {
	return gcmSessionCreator{}
}

func (gcmSessionCreator) Create(peerID protocol.PeerID, secret []byte) protocol.Session {
	secret32 := [32]byte{}
	copy(secret32[:], secret)
	return NewGCMSession(peerID, secret32)
}

func (gcmSessionCreator) SecretLength() int {
	return 32
}

type gcmSession struct {
	gcm    cipher.AEAD
	key    []byte
	rand   *rand.Rand
	peerID protocol.PeerID
}

func NewGCMSession(peerID protocol.PeerID, secret [32]byte) protocol.Session {
	block, err := aes.NewCipher(secret[:])
	if err != nil {
		panic(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	seed := binary.BigEndian.Uint64(secret[:8])
	return &gcmSession{
		gcm:    gcm,
		key:    secret[:],
		rand:   rand.New(rand.NewSource(int64(seed))),
		peerID: peerID,
	}
}

func (session *gcmSession) ReadMessage(reader io.Reader) (protocol.MessageOnTheWire, error) {
	motw := protocol.MessageOnTheWire{}
	motw.From = session.peerID
	msg, err := protocol.ReadMessage(reader)
	if err != nil {
		return motw, err
	}
	motw.Message = msg
	nonce := make([]byte, session.gcm.NonceSize())
	session.rand.Read(nonce)
	motw.Message.Body, err = session.gcm.Open(nil, nonce, motw.Message.Body, nil)
	if err != nil {
		return motw, err
	}
	motw.Message.Length = protocol.MessageLength(len(motw.Message.Body) + 8)
	return motw, nil
}

func (session *gcmSession) WriteMessage(msg protocol.Message, writer io.Writer) error {
	nonce := make([]byte, session.gcm.NonceSize())
	session.rand.Read(nonce)
	msg.Body = session.gcm.Seal(nil, nonce, msg.Body, nil)
	msg.Length = protocol.MessageLength(len(msg.Body) + 8)
	msgData, err := msg.MarshalBinary()
	if err != nil {
		return err
	}
	if n, err := writer.Write(msgData); n != len(msgData) || err != nil {
		return fmt.Errorf("failed to write data to the writer: size: %v written: %v : %v", len(msgData), n, err)
	}
	return nil
}
