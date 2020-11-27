package peer

import (
	"context"
	"sync"

	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

type syncRc struct {
	n  int
	ch chan []byte
}

type Syncer struct {
	next Receiver

	syncingMu *sync.Mutex
	syncing   map[string]syncRc

	table     Table
	transport *transport.Transport
}

func NewSyncer(next Receiver) *Syncer {
	return &Syncer{
		next: next,
	}
}

func (syncer *Syncer) Sync(ctx context.Context, contentID []byte, from *id.Signatory) ([]byte, error) {
	syncer.syncingMu.Lock()
	rc, ok := syncer.syncing[string(contentID)]
	if ok {
		rc.n++
		syncer.syncing[string(contentID)] = rc
	}
	syncer.syncingMu.Unlock()

	if ok {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case content := <-rc.ch:
			return content, nil
		}
	}
}

func (syncer *Syncer) DidReceiveMessage(from id.Signatory, msg wire.Msg) {
	switch msg.Type {
	case wire.MsgTypeSync:
	}
	syncer.next.DidReceiveMessage(from, msg)
}
