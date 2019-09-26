package protocol

import "io"

type Session interface {
	ReadMessage(io.Reader) (MessageOnTheWire, error)
	WriteMessage(Message, io.Writer) error
}

type SessionCreator interface {
	Create(PeerID, []byte) Session
	SecretLength() int
}
