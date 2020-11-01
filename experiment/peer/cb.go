package peer

import (
	"github.com/renproject/aw/experiment/wire"
	"github.com/renproject/id"
)

type Callbacks struct {
	DidReceiveMessage func(from id.Signatory, msg wire.Msg)
}
