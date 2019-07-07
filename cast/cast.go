package cast

import "context"

type Caster interface {
	Cast(ctx context.Context, to protocol.PeerID, message protocol.Message) error
}

type caster struct {
	sender protocol.MessageSender
	dht    dht.DHT
}

func NewCaster(sender protocol.MessageSender) Caster {
	return &caster{
		sender: sender,
	}
}

func (caster *caster) Cast(ctx context.Context, to protocol.PeerID, message protocol.Message) error {

}
