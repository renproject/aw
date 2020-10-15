package channel

import (
	"net"
	"time"

	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/id"
)

type Channel struct {
	connectedAt time.Time

	self    id.Signatory
	remote  id.Signatory
	conn    net.Conn
	encoder codec.Encoder
	decoder codec.Decoder
}

func (ch Channel) ConnectedAt() time.Time {
	return ch.connectedAt
}

func (ch Channel) Write(buf []byte) (int, error) {
	return ch.encoder(ch.conn, buf)
}

func (ch Channel) Read(buf []byte) (int, error) {
	return ch.decoder(ch.conn, buf)
}

func (ch Channel) Self() id.Signatory {
	return ch.self
}

func (ch Channel) Remote() id.Signatory {
	return ch.remote
}

func (ch Channel) Close() error {
	return ch.conn.Close()
}
