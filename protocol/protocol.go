package protocol

import (
	"context"
	"io"
)

type SignVerifier interface {
	Sign(digest []byte) ([]byte, error)
	Verify(digest, sig []byte) (PeerID, error)
	Hash(data []byte) []byte
	SigLength() uint64
}

type Session interface {
	ReadMessageOnTheWire(io.Reader) (MessageOnTheWire, error)
	WriteMessage(io.Writer, Message) error
}

type SessionManager interface {
	NewSession(PeerID, []byte) Session
	NewSessionKey() []byte
}

type Client interface {
	Run(context.Context, MessageReceiver)
}

type Server interface {
	Run(context.Context, MessageSender)
}
