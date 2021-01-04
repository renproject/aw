package channel

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	"go.uber.org/zap"
)

type receiver struct {
	ctx context.Context
	f   func(id.Signatory, wire.Msg) error
}

type sharedChannel struct {
	// ch defines a channel that is bound to a remote peer.
	ch *Channel
	// rc defines a reference-counter that tracks the number of references
	// currently bound.
	rc uint64
	// cancel the running channel.
	cancel context.CancelFunc
	// inbound channel receives messages from the remote peer to which the
	// channel is bound.
	inbound <-chan wire.Msg
	// outbound channel is sent messages that are destined for the remote peer
	// to which the channel is bound.
	outbound chan<- wire.Msg
}

type Msg struct {
	wire.Msg
	From id.Signatory
}

type Client struct {
	opts Options
	self id.Signatory

	sharedChannelsMu *sync.RWMutex
	sharedChannels   map[id.Signatory]*sharedChannel

	inbound            chan Msg
	receivers          chan receiver
	receiversRunningMu *sync.Mutex
	receiversRunning   bool
}

func NewClient(opts Options, self id.Signatory) *Client {
	return &Client{
		opts: opts,
		self: self,

		sharedChannelsMu: new(sync.RWMutex),
		sharedChannels:   map[id.Signatory]*sharedChannel{},

		inbound:            make(chan Msg),
		receivers:          make(chan receiver),
		receiversRunningMu: new(sync.Mutex),
		receiversRunning:   false,
	}
}

func (client *Client) Bind(remote id.Signatory) {
	client.sharedChannelsMu.Lock()
	defer client.sharedChannelsMu.Unlock()

	shared, ok := client.sharedChannels[remote]
	if ok {
		shared.rc++
		return
	}

	inbound := make(chan wire.Msg, client.opts.InboundBufferSize)
	outbound := make(chan wire.Msg, client.opts.OutboundBufferSize)

	ctx, cancel := context.WithCancel(context.Background())
	ch := New(client.opts, remote, inbound, outbound)
	go func() {
		defer close(inbound)
		if err := ch.Run(ctx); err != nil {
			client.opts.Logger.Error("run", zap.Error(err))
		}
	}()
	go func() {
		for msg := range inbound {
			select {
			case <-ctx.Done():
			case client.inbound <- Msg{Msg: msg, From: remote}:
			}
		}
	}()

	client.sharedChannels[remote] = &sharedChannel{
		ch:       ch,
		rc:       1,
		cancel:   cancel,
		inbound:  inbound,
		outbound: outbound,
	}
}

func (client *Client) Unbind(remote id.Signatory) {
	client.sharedChannelsMu.Lock()
	defer client.sharedChannelsMu.Unlock()

	shared, ok := client.sharedChannels[remote]
	if !ok {
		return
	}

	shared.rc--
	if shared.rc == 0 {
		shared.cancel()
		delete(client.sharedChannels, remote)
	}
}

func (client *Client) IsBound(remote id.Signatory) bool {
	client.sharedChannelsMu.RLock()
	defer client.sharedChannelsMu.RUnlock()

	return client.sharedChannels[remote].rc > 0
}

// Attach a network connection, encoder, and decoder to the Channel associated
// with a remote peer without incrementing the reference-counter of the Channel.
// An error is returned if no Channel is associated with the remote peer. As
// with the Attach method that is exposed directly by a Channel, this method is
// blocking.
func (client *Client) Attach(ctx context.Context, remote id.Signatory, conn net.Conn, enc codec.Encoder, dec codec.Decoder) error {
	client.sharedChannelsMu.RLock()
	shared, ok := client.sharedChannels[remote]
	if !ok {
		client.sharedChannelsMu.RUnlock()
		return fmt.Errorf("attach: no connection to %v", remote)
	}
	client.sharedChannelsMu.RUnlock()

	client.opts.Logger.Debug("attach", zap.String("self", client.self.String()), zap.String("remote", remote.String()), zap.String("addr", conn.RemoteAddr().String()))
	if err := shared.ch.Attach(ctx, remote, conn, enc, dec); err != nil {
		return fmt.Errorf("attach: %v", err)
	}
	return nil
}

func (client *Client) Send(ctx context.Context, remote id.Signatory, msg wire.Msg) error {
	client.sharedChannelsMu.RLock()
	shared, ok := client.sharedChannels[remote]
	if !ok {
		client.sharedChannelsMu.RUnlock()
		return fmt.Errorf("channel not found: %v", remote)
	}
	client.sharedChannelsMu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case shared.outbound <- msg:
		return nil
	}
}

func (client *Client) Receive(ctx context.Context, f func(id.Signatory, wire.Msg) error) {
	client.receiversRunningMu.Lock()
	if client.receiversRunning {
		client.receiversRunningMu.Unlock()
		select {
		case <-ctx.Done():
		case client.receivers <- receiver{ctx: ctx, f: f}:
		}
		return
	}
	client.receiversRunning = true
	client.receiversRunningMu.Unlock()

	go func() {
		receivers := []receiver{}

		for {
			select {
			case receiver := <-client.receivers:
				// A new receiver has been registered.
				receivers = append(receivers, receiver)
			case msg := <-client.inbound:
				marker := 0
				for _, receiver := range receivers {
					select {
					case <-receiver.ctx.Done():
						// Do nothing. This will implicitly mark it for
						// deletion.
					default:
						if err := receiver.f(msg.From, msg.Msg); err != nil {
							// When a channel is killed, its context will be
							// cancelled, its underlying network connections
							// will be dropped, and sending will fail. A killed
							// channel can only be revived by completely
							// unbinding all references, and binding a new
							// reference.
							client.opts.Logger.Error("filter", zap.String("remote", msg.From.String()), zap.Error(err))
							client.sharedChannelsMu.Lock()
							if shared, ok := client.sharedChannels[msg.From]; ok {
								shared.cancel()
							}
							client.sharedChannelsMu.Unlock()
						}
						receivers[marker] = receiver
						marker++
					}
				}
				// Delete everything that was marked for deletion.
				for del := marker; del < len(receivers); del++ {
					receivers[del] = receiver{}
				}
				receivers = receivers[:marker]
			}
			if len(receivers) == 0 {
				break
			}
		}

		client.receiversRunningMu.Lock()
		client.receiversRunning = false
		client.receiversRunningMu.Unlock()
	}()

	select {
	case <-ctx.Done():
	case client.receivers <- receiver{ctx: ctx, f: f}:
	}
}
