package tcp

import (
	"github.com/renproject/aw/protocol"
)

type Client struct {
	receiver protocol.MessageReceiver
}

func NewClient(receiver protocol.MessageReceiver) *Client {
	return &Client{
		receiver: receiver,
	}
}
