package handshake

import (
	"fmt"
	"io"

	"github.com/renproject/aw/protocol"
)

type insecureSessionManager struct{}

// NewInsecureSessionManager returns a `protocol.SessionManager` that does no
// ecryption/authentication. Messages are read/written from/to connections as
// without modification or processing.
func NewInsecureSessionManager() protocol.SessionManager {
	return insecureSessionManager{}
}

func (insecureSessionManager) NewSession(peerID protocol.PeerID, secret []byte) protocol.Session {
	return newInsecureSession(peerID)
}

func (insecureSessionManager) NewSessionKey() []byte {
	return []byte{}
}

type insecureSession struct {
	peerID protocol.PeerID
}

func newInsecureSession(peerID protocol.PeerID) protocol.Session {
	return &insecureSession{peerID: peerID}
}

func (session *insecureSession) ReadMessageOnTheWire(r io.Reader) (protocol.MessageOnTheWire, error) {
	otw := protocol.MessageOnTheWire{}
	otw.From = session.peerID
	err := otw.Message.UnmarshalReader(r)
	return otw, err
}

func (session *insecureSession) WriteMessage(w io.Writer, message protocol.Message) error {
	data, err := message.MarshalBinary()
	if err != nil {
		return err
	}
	n, err := w.Write(data)
	if n != len(data) {
		return fmt.Errorf("error writing message: expected n=%v, got n=%v", len(data), n)
	}
	return err
}
