package handshake

import (
	"context"
	"net"

	"github.com/renproject/id"
)

type Handshaker interface {
	// Handshake with a remote server by initiating, and then interactively
	// completing, a handshake protocol. The remote server is accessed by
	// reading/writing to the `io.ReaderWriter`.
	Handshake(ctx context.Context, c net.Conn) (Session, error)

	// AcceptHandshake from a remote client by waiting for the initiation of,
	// and then interactively completing, a handshake protocol. The remote
	// client is accessed by reading/writing to the `io.ReaderWriter`.
	AcceptHandshake(ctx context.Context, c net.Conn) (Session, error)
}

// A Filter for identities that are established during a handshake. Although
// the handshake may be successful, the actual identity may want to be rejected
// because of some application-level logic. Filters are a hook for implementing
// such logic.
type Filter interface {
	Filter(id.Signatory) bool

	// FIXME: There needs to be a "unfilter" method available on this interface.
	// This should be called when the connection is closed, so that the filter
	// can reset any state necessary.
	//
	//  Unfilter(id.Signatory)
	//
}

type Session interface {
	// Encrypt data for the other Signatory.
	Encrypt([]byte) ([]byte, error)
	// Decrypt data from the other Signatory.
	Decrypt([]byte) ([]byte, error)
	// RemoteSignatory returns the pubkey hash of the Signatory that is on the
	// remote end of the Session.
	RemoteSignatory() id.Signatory

	// FIXME: There needs to be a "close" method available on this interface.
	// This should be called when the connection is closed, so that the session can
	// close the filter.
	//
	//	Close()
	//
}
