package peer

import (
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

type Callbacks struct {
	DidReceiveMessage func(from id.Signatory, msg wire.Msg)
}

func DefaultDidReceiveMessage(from id.Signatory, msg wire.Msg) {

}
