package handshake

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type insecureHandshaker struct {
	opts Options
}

// NewInsecure returns a new Handshaker that implements no authentication,
// encryption, or restrictions on connections.
func NewInsecure(opts Options) Handshaker {
	return &insecureHandshaker{opts: opts}
}

// Handshake with a remote server. The input ReadWriter is returned without
// modification.
func (handshaker *insecureHandshaker) Handshake(ctx context.Context, conn net.Conn) (Session, error) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	//
	// Write own client identity.
	//

	conn.SetWriteDeadline(time.Now().Add(handshaker.opts.Timeout / 2))
	if _, err := id.NewSignatory(&handshaker.opts.PrivKey.PublicKey).Marshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("marshaling identity: %v", err)
	}

	//
	// Read remote server identity.
	//

	serverSignatory := id.Signatory{}
	conn.SetReadDeadline(time.Now().Add(handshaker.opts.Timeout / 2))
	if _, err := serverSignatory.Unmarshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("unmarshaling server identity: %v", err)
	}
	if handshaker.opts.Filter != nil && !handshaker.opts.Filter.Filter(serverSignatory) {
		return nil, fmt.Errorf("filtering: bad server")
	}

	return insecureSession{remoteSignatory: serverSignatory}, nil
}

// AcceptHandshake from a remote client. The input ReadWriter is returned
// without modification.
func (handshaker *insecureHandshaker) AcceptHandshake(ctx context.Context, conn net.Conn) (Session, error) {
	defer conn.SetDeadline(time.Time{})

	//
	// Read remote client identity.
	//

	clienSignatory := id.Signatory{}
	conn.SetReadDeadline(time.Now().Add(handshaker.opts.Timeout / 2))
	if _, err := clienSignatory.Unmarshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("unmarshaling client identity: %v", err)
	}
	if handshaker.opts.Filter != nil && !handshaker.opts.Filter.Filter(clienSignatory) {
		return nil, fmt.Errorf("filtering: bad server")
	}

	//
	// Write own server identity.
	//

	conn.SetWriteDeadline(time.Now().Add(handshaker.opts.Timeout / 2))
	if _, err := id.NewSignatory(&handshaker.opts.PrivKey.PublicKey).Marshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("marshaling identity: %v", err)
	}

	return insecureSession{remoteSignatory: clienSignatory}, nil
}

type insecureSession struct {
	remoteSignatory id.Signatory
}

func (session insecureSession) Encrypt(p []byte) ([]byte, error) {
	return p, nil
}

func (session insecureSession) Decrypt(p []byte) ([]byte, error) {
	return p, nil
}

func (session insecureSession) RemoteSignatory() id.Signatory {
	return session.remoteSignatory
}
