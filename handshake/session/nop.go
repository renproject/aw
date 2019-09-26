package session

import (
	"fmt"
	"io"

	"github.com/renproject/aw/protocol"
)

type nopSessionCreator struct{}

func NewNOPSessionCreator() protocol.SessionCreator {
	return nopSessionCreator{}
}

func (nopSessionCreator) Create(peerID protocol.PeerID, secret []byte) protocol.Session {
	return NewNOPSession(peerID)
}

func (nopSessionCreator) SecretLength() int {
	return 32
}

type nopSession struct {
	peerID protocol.PeerID
}

func NewNOPSession(peerID protocol.PeerID) protocol.Session {
	return &nopSession{peerID: peerID}
}

func (session *nopSession) ReadMessage(reader io.Reader) (protocol.MessageOnTheWire, error) {
	motw := protocol.MessageOnTheWire{}
	motw.From = session.peerID
	msg, err := protocol.ReadMessage(reader)
	if err != nil {
		return motw, err
	}
	motw.Message = msg
	return motw, nil
}

func (session *nopSession) WriteMessage(msg protocol.Message, writer io.Writer) error {
	msgData, err := msg.MarshalBinary()
	if err != nil {
		return err
	}
	if n, err := writer.Write(msgData); n != len(msgData) || err != nil {
		return fmt.Errorf("failed to write data to the writer: size: %v written: %v : %v", len(msgData), n, err)
	}
	return nil
}
