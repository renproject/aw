package protocol

import (
	"context"
	"io"

	"github.com/renproject/phi"
)

type SignVerifier interface {
	Sign(digest []byte) ([]byte, error)
	Verify(digest, sig []byte) (PeerID, error)
	Hash(data []byte) []byte
	// PrivateKey() ecdsa.PrivateKey
	// PublicKey () ecdsa.PublicKey
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
