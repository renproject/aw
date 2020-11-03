package peer

import (
	"github.com/renproject/experiment/wire"
	"github.com/renproject/id"
)

type Callbacks struct {
	DidReceiveMessage func(from id.Signatory, msg wire.Msg)
}
