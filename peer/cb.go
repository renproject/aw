package peer

import (
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

type Callbacks struct {
	DidReceiveMessage func(p *Peer, from id.Signatory, msg wire.Msg)
}