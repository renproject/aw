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
	self    id.Signatory
	timeout time.Duration
}

// NewInsecure returns a new Handshaker that implements no authentication,
// encryption, or restrictions on connections.
func NewInsecure(self id.Signatory, timeout time.Duration) Handshaker {
	return &insecureHandshaker{self: self, timeout: timeout}
}

// Handshake with a remote server. The input ReadWriter is returned without
// modification.
func (h *insecureHandshaker) Handshake(ctx context.Context, conn net.Conn) (Session, error) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	other := id.Signatory{}

	conn.SetWriteDeadline(time.Now().Add(h.timeout / 2))
	if _, err := h.self.Marshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("marshaling self: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(h.timeout / 2))
	if _, err := other.Unmarshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("unmarshaling other: %v", err)
	}

	return insecureSession{other: other}, nil
}

// AcceptHandshake from a remote client. The input ReadWriter is returned
// without modification.
func (h *insecureHandshaker) AcceptHandshake(ctx context.Context, conn net.Conn) (Session, error) {
	defer conn.SetDeadline(time.Time{})

	other := id.Signatory{}

	conn.SetReadDeadline(time.Now().Add(h.timeout / 2))
	if _, err := other.Unmarshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("unmarshaling other: %v", err)
	}
	conn.SetWriteDeadline(time.Now().Add(h.timeout / 2))
	if _, err := h.self.Marshal(conn, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("marshaling self: %v", err)
	}

	return insecureSession{other: other}, nil
}

type insecureSession struct {
	other id.Signatory
}

func (session insecureSession) Encrypt(p []byte) ([]byte, error) {
	return p, nil
}

func (session insecureSession) Decrypt(p []byte) ([]byte, error) {
	return p, nil
}

func (session insecureSession) Signatory() id.Signatory {
	return session.other
}
