package peer

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

var (
	DefaultAlpha   = 5
	DefaultTimeout = time.Second
)

type SyncerOptions struct {
	Logger  *zap.Logger
	Alpha   int
	Timeout time.Duration
}

func DefaultSyncerOptions() SyncerOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return SyncerOptions{
		Logger:  logger,
		Alpha:   DefaultAlpha,
		Timeout: DefaultTimeout,
	}
}

func (opts SyncerOptions) WithLogger(logger *zap.Logger) SyncerOptions {
	opts.Logger = logger
	return opts
}

func (opts SyncerOptions) WithAlpha(alpha int) SyncerOptions {
	opts.Alpha = alpha
	return opts
}

func (opts SyncerOptions) WithTimeout(timeout time.Duration) SyncerOptions {
	opts.Timeout = timeout
	return opts
}

type pendingContent struct {
	// content is nil while synchronisation is happening. After synchronisation
	// has completed, content will be set.
	content []byte

	// cond is used to wait and notify goroutines about the completion of
	// synchronisation.
	cond *sync.Cond
}

// wait for the content to be synchronised. Returns a channel that the caller
// can block on while waiting for the content. This is better than blocking
// interally, because it can be composed with contexts/timeouts.
func (pending *pendingContent) wait() <-chan []byte {
	w := make(chan []byte, 1)
	go func() {
		pending.cond.L.Lock()
		for pending.content == nil {
			pending.cond.Wait()
		}
		content := make([]byte, len(pending.content), len(pending.content))
		copy(content, pending.content)
		pending.cond.L.Unlock()
		w <- content
	}()
	return w
}

// signal that the content is synchronised. All goroutines waiting on the
// content will be awaken and will create their own copies of the content.
func (pending *pendingContent) signal(content []byte) {
	pending.cond.L.Lock()
	pending.content = content
	pending.cond.L.Unlock()
}

type Syncer struct {
	opts      SyncerOptions
	filter    *channel.SyncFilter
	transport *transport.Transport

	pendingMu *sync.Mutex
	pending   map[string]pendingContent

	next Receiver
}

func NewSyncer(opts SyncerOptions, filter *channel.SyncFilter, transport *transport.Transport, next Receiver) *Syncer {
	return &Syncer{
		opts:      opts,
		filter:    filter,
		transport: transport,

		pendingMu: new(sync.Mutex),
		pending:   make(map[string]pendingContent, 1024),

		next: next,
	}
}

func (syncer *Syncer) Sync(ctx context.Context, contentID []byte, from *id.Signatory) ([]byte, error) {
	syncer.pendingMu.Lock()
	pending, ok := syncer.pending[string(contentID)]
	if !ok {
		pending = pendingContent{
			content: nil,
			cond:    sync.NewCond(new(sync.Mutex)),
		}
		syncer.pending[string(contentID)] = pending
	}
	syncer.pendingMu.Unlock()

	// Allow synchronisation messages for the content ID. This is required in
	// order for channel to not filter inbound content (of unknown size). At the
	// end of the method, we Deny the content ID again, un-doing the Allow and
	// blocking content again.
	syncer.filter.Allow(contentID)
	defer syncer.filter.Deny(contentID)

	if ok {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case content := <-pending.wait():
			return content, nil
		}
	}

	// Get addresses close to our address. We will iterate over these addresses
	// in order and attempt to synchronise content by sending them pull
	// messages.
	peers := syncer.transport.Table().Addresses(syncer.opts.Alpha)
	if from != nil {
		peers = append([]id.Signatory{*from}, peers...)
	}

	for _, peer := range peers {
		content, err := func() ([]byte, error) {
			ctx, cancel := context.WithTimeout(ctx, syncer.opts.Timeout)
			defer cancel()

			err := syncer.transport.Send(ctx, peer, wire.Msg{
				Version: wire.MsgVersion1,
				Type:    wire.MsgTypePull,
				Data:    contentID,
			})
			if err != nil {
				return nil, fmt.Errorf("pulling: %v", err)
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case content := <-pending.wait():
				return content, nil
			}
		}()
		if err != nil {
			syncer.opts.Logger.Debug("sync", zap.String("peer", peer.String()), zap.Error(err))
			continue
		}
		return content, nil
	}

	return nil, fmt.Errorf("content not found: %v", base64.RawURLEncoding.EncodeToString(contentID))
}

func (syncer *Syncer) DidReceiveMessage(from id.Signatory, msg wire.Msg) {
	if msg.Type == wire.MsgTypeSync {
		syncer.pendingMu.Lock()
		pending, ok := syncer.pending[string(msg.Data)]
		if ok && msg.SyncData != nil {
			pending.signal(msg.SyncData)
		}
		syncer.pendingMu.Unlock()
	}
	syncer.next.DidReceiveMessage(from, msg)
}