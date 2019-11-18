package tcp

import (
	"context"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Client struct {
	logger   logrus.FieldLogger
	pool     ConnPool
	messages protocol.MessageReceiver
}

func NewClient(logger logrus.FieldLogger, pool ConnPool, messages protocol.MessageReceiver) *Client {
	return &Client{
		logger:   logger,
		pool:     pool,
		messages: messages,
	}
}

func (client *Client) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageOtw := <-client.messages:
			client.handleMessageOnTheWire(messageOtw)
		}
	}
}

func (client *Client) handleMessageOnTheWire(message protocol.MessageOnTheWire) {
	if err := client.pool.Send(message.To.NetworkAddress(), message.Message); err != nil {
		client.logger.Errorf("error writing to %v: %v", message.To.NetworkAddress(), err)
		return
	}
}
