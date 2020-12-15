package channel

import (
	"sync"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// A Filter is used to drop messages, and their respective channels, when the
// messages are unexpected or malicious.
type Filter interface {
	Filter(id.Signatory, wire.Msg) bool
}

// FilterFunc is a wrapper around a function that implements the Filter
// interface.
type FilterFunc func(id.Signatory, wire.Msg) bool

func (f FilterFunc) Filter(from id.Signatory, msg wire.Msg) bool {
	return f(from, msg)
}

// A SyncFilter is used to filter synchronisation messages. If the local peer
// is not expecting to receive a synchronisation message, then the message will
// be filtered and the respective channel will be dropped.
type SyncFilter struct {
	expectingMu *sync.RWMutex
	expecting   map[string]int
}

// NewSyncFilter returns a new filter where all content IDs are denied by
// default, and no allowances exist.
func NewSyncFilter() *SyncFilter {
	return &SyncFilter{
		expectingMu: new(sync.RWMutex),
		expecting:   make(map[string]int, 1000),
	}
}

// Allow synchronisation messages for the given content ID. Every call to Allow
// must be eventually followed by a call to Deny. By default, all content IDs
// are denied until Allow is called.
func (f *SyncFilter) Allow(contentID []byte) {
	f.expectingMu.Lock()
	defer f.expectingMu.Unlock()

	f.expecting[string(contentID)]++
}

// Deny synchronisation messages for the given content ID. Denying a content ID
// reverses one call to Allow. If there are no calls to Allow, this method does
// nothing.
func (f *SyncFilter) Deny(contentID []byte) {
	f.expectingMu.Lock()
	defer f.expectingMu.Unlock()

	contentIDAsString := string(contentID)
	if f.expecting[contentIDAsString] == 1 {
		delete(f.expecting, contentIDAsString)
		return
	}
	f.expecting[contentIDAsString]--
}

// Filter returns true if the message is not a synchronisation message, or the
// content ID is not expected.
func (f *SyncFilter) Filter(from id.Signatory, msg wire.Msg) bool {
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
