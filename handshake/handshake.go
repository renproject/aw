package handshake

import (
	"context"
	"io"

	"github.com/renproject/id"
)

type Handshaker interface {
	// Handshake with a remote server by initiating, and then interactively
	// completing, a handshake protocol. The remote server is accessed by
	// reading/writing to the `io.ReaderWriter`.
	Handshake(ctx context.Context, rw io.ReadWriter) (Session, error)

	// AcceptHandshake from a remote client by waiting for the initiation of,
	// and then interactively completing, a handshake protocol. The remote
	// client is accessed by reading/writing to the `io.ReaderWriter`.
	AcceptHandshake(ctx context.Context, rw io.ReadWriter) (Session, error)
}

// A Verifier filters identities that are authenticated during a handshake.
// Although the handshake may be successful, the actual identity may want to be
// rejected because of some application-logic. Verifiers are a hook for
// implementing such logic.
type Verifier interface {
	Verify(id.Signatory) bool
}

type Session interface {
	Encrypt([]byte) ([]byte, error)
	Decrypt([]byte) ([]byte, error)
}
