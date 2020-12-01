package channel

import (
	"sync"

	"github.com/renproject/aw/wire"
)

// A SyncFilter is used to filter synchronisation messages sent by remote peers.
// If the local peer is not expecting to receive a synchronisation message, then
// the message will be filtered and the respective channel will be dropped.
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
	if f.expecting[contentIDAsString] > 1 {
		delete(f.expecting, contentIDAsString)
		return
	}
	f.expecting[contentIDAsString]--
}

// Filter returns true if the message is not a synchronisation message, or the
// content ID has one or more outstanding allowance.
func (f *SyncFilter) Filter(msg wire.Msg) bool {
	if msg.Type != wire.MsgTypeSync {
		return true
	}

	f.expectingMu.RLock()
	defer f.expectingMu.RUnlock()

	return f.expecting[string(msg.Data)] > 0
}