package aw

import (
	"sync"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// A SyncFilter is used to filter synchronisation messages. If the local peer
// is not expecting to receive a synchronisation message, then the message will
// be filtered and the respective channel will be dropped.
type syncFilter struct {
	expectingMu *sync.RWMutex
	expecting   map[string]int
}

// newSyncFilter returns a new filter where all content IDs are denied by
// default, and no allowances exist.
func newSyncFilter() *syncFilter {
	return &syncFilter{
		expectingMu: new(sync.RWMutex),
		expecting:   make(map[string]int, 1000),
	}
}

// allow synchronisation messages for the given content ID. Every call to allow
// must be eventually followed by a call to deny. By default, all content IDs
// are denied until allow is called.
func (f *syncFilter) allow(contentID []byte) {
	f.expectingMu.Lock()
	defer f.expectingMu.Unlock()

	f.expecting[string(contentID)]++
}

// deny synchronisation messages for the given content ID. Denying a content ID
// reverses one call to allow. If there are no calls to allow, this method does
// nothing.
func (f *syncFilter) deny(contentID []byte) {
	f.expectingMu.Lock()
	defer f.expectingMu.Unlock()

	contentIDAsString := string(contentID)
	if f.expecting[contentIDAsString] == 1 {
		delete(f.expecting, contentIDAsString)
		return
	}
	f.expecting[contentIDAsString]--
}

// filter returns true if the message is not a synchronisation message, or the
// content ID is not expected.
func (f *syncFilter) filter(from id.Signatory, msg wire.Msg) bool {
	if msg.Type != wire.MsgTypeSync {
		return true
	}

	f.expectingMu.RLock()
	defer f.expectingMu.RUnlock()

	_, ok := f.expecting[string(msg.Data)]
	if !ok {
		return true
	}
	return false
}
