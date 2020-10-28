package peer

import "github.com/renproject/id"

type Message struct {
}

type MessageHeader struct {
	Version       uint16
	ContentType   uint16
	ContentLength uint32
	ContentHash   id.Hash
}
