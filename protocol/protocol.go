package protocol

import (
	"io"
)

type SignVerifier interface {
	Sign(digest []byte) ([]byte, error)
	Verify(digest, sig []byte) (PeerID, error)
	Hash(data []byte) []byte
	SigLength() uint64
}

type Session interface {
	ReadMessage(io.Reader) (MessageOnTheWire, error)
	WriteMessage(Message, io.Writer) error
}

type SessionCreator interface {
	Create(PeerID, []byte) Session
	SecretLength() int
}
