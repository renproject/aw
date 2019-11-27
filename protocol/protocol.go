package protocol

import (
	"context"
	"io"

	"github.com/renproject/phi"
)

// SignVerifier is used for authentication by using cryptography.
type SignVerifier interface {
	Sign(digest []byte) ([]byte, error)
	Verify(digest, sig []byte) (PeerID, error)
	Hash(data []byte) []byte
	SigLength() uint64
}

// Session is a secure connection between two Peers which allow them send and
// receive AW messages through the same io.ReadWriter.
type Session interface {
	ReadMessageOnTheWire(io.Reader) (MessageOnTheWire, error)
	WriteMessage(io.Writer, Message) error
}

// SessionManager is able to establish new Session with a Peer.
type SessionManager interface {
	NewSession(peerID PeerID, key []byte) Session
	NewSessionKey() []byte
}

// Client reads message from the MessageReceiver and sends the message to the
// target Peer.
type Client interface {
	Run(context.Context, MessageReceiver)
}

// Server listens for messages sent by other Peers and pipes it to the message
// handler through the provided MessageSender
type Server interface {
	Run(context.Context, MessageSender)
}

// Spawn multiple goroutine workers to process the peer addresses in the queue one-by-one.
func ParForAllAddresses(addrs PeerAddresses, numWorkers int, f func(PeerAddress)) {
	peerAddrsQ := make(chan PeerAddress, len(addrs))
	for _, peerAddr := range addrs {
		peerAddrsQ <- peerAddr
	}
	close(peerAddrsQ)

	phi.ForAll(numWorkers, func(_ int) {
		for peerAddr := range peerAddrsQ {
			f(peerAddr)
		}
	})
}
