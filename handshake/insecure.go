package handshake

import (
	"context"
	"fmt"
	"io"

	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type insecureHandshaker struct {
	self id.Signatory
}

// NewInsecure returns a new Handshaker that implements no authentication,
// encryption, or restrictions on connections.
func NewInsecure(self id.Signatory) Handshaker {
	return &insecureHandshaker{self: self}
}

// Handshake with a remote server. The input ReadWriter is returned without
// modification.
func (h *insecureHandshaker) Handshake(ctx context.Context, rw io.ReadWriter) (Session, error) {
	if _, err := h.self.Marshal(rw, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("marshaling self: %v", err)
	}
	other := id.Signatory{}
	if _, err := other.Unmarshal(rw, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("unmarshaling other: %v", err)
	}
	return insecureSession{other: other}, nil
}

// AcceptHandshake from a remote client. The input ReadWriter is returned
// without modification.
func (h *insecureHandshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (Session, error) {
	other := id.Signatory{}
	if _, err := other.Unmarshal(rw, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("unmarshaling other: %v", err)
	}
	if _, err := h.self.Marshal(rw, surge.MaxBytes); err != nil {
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
