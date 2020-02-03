package handshake

import (
	"context"
	"io"
)

type insecureSession struct {
	inner io.ReadWriter
}

func (session insecureSession) Encrypt(p []byte) ([]byte, error) {
	return p, nil
}

func (session insecureSession) Decrypt(p []byte) ([]byte, error) {
	return p, nil
}

type insecureHandshaker struct {
}

// NewInsecure returns a new Handshaker that implements no authentication,
// encryption, or restrictions on connections.
func NewInsecure() Handshaker {
	return &insecureHandshaker{}
}

// Handshake with a remote server. The input io.ReadWriter is returned without
// modification.
func (h *insecureHandshaker) Handshake(ctx context.Context, rw io.ReadWriter) (Session, error) {
	return insecureSession{rw}, nil
}

// AcceptHandshake from a remote client. The input io.ReadWriter is returned
// without modification.
func (h *insecureHandshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (Session, error) {
	return insecureSession{rw}, nil
}
