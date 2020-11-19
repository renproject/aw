package channel

import (
	"context"
	"fmt"
	"github.com/renproject/aw/dht"
	"net"
	"sync"

	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type fanOutReceiver struct {
	ctx      context.Context
	receiver chan<- Msg
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

var (
	DefaultInboundBufferSize  = 0
	DefaultOutboundBufferSize = 0
)

type ClientOptions struct {
	Logger             *zap.Logger
	InboundBufferSize  int
	OutboundBufferSize int
	ChannelOptions     Options
}

func DefaultClientOptions() ClientOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return ClientOptions{
		Logger:             logger,
		InboundBufferSize:  DefaultInboundBufferSize,
		OutboundBufferSize: DefaultOutboundBufferSize,
		ChannelOptions:     DefaultOptions(),
	}
}

func (opts ClientOptions) WithLogger(logger *zap.Logger) ClientOptions {
	opts.Logger = logger
	return opts
}

func (opts ClientOptions) WithChannelOptions(channelOpts Options) ClientOptions {
	opts.ChannelOptions = channelOpts
	return opts
}

type Msg struct {
	wire.Msg
	From id.Signatory
}

type Client struct {
	opts ClientOptions
	self id.Signatory

	sharedChannelsMu *sync.RWMutex
	sharedChannels   map[id.Signatory]*sharedChannel

	inbound         chan Msg
	fanOutReceivers chan fanOutReceiver
	fanOutRunningMu *sync.Mutex
	fanOutRunning   bool

	contentResolver dht.ContentResolver
	msgFollowup func(msgType uint16) bool
}

func NewClient(opts ClientOptions, self id.Signatory, contentResolver dht.ContentResolver, msgFollowup func(msgType uint16) bool) *Client {
	return &Client{
		opts: opts,
		self: self,

		sharedChannelsMu: new(sync.RWMutex),
		sharedChannels:   map[id.Signatory]*sharedChannel{},

		inbound:         make(chan Msg),
		fanOutReceivers: make(chan fanOutReceiver),
		fanOutRunningMu: new(sync.Mutex),
		fanOutRunning:   false,

		contentResolver: contentResolver,
		msgFollowup: msgFollowup,
	}
}

func (client *Client) Bind(remote id.Signatory) {
	client.sharedChannelsMu.Lock()
	defer client.sharedChannelsMu.Unlock()

	sc, ok := client.sharedChannels[remote]
	if ok {
		sc.rc++
		return
	}

	inbound := make(chan wire.Msg, client.opts.InboundBufferSize)
	outbound := make(chan wire.Msg, client.opts.OutboundBufferSize)

	ctx, cancel := context.WithCancel(context.Background())
	ch := New(client.opts.ChannelOptions, remote, inbound, outbound, client.contentResolver, client.msgFollowup)
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

	sc, ok := client.sharedChannels[remote]
	if !ok {
		return
	}

	sc.rc--
	if sc.rc == 0 {
		sc.cancel()
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
	sc, ok := client.sharedChannels[remote]
	if !ok {
		client.sharedChannelsMu.RUnlock()
		return fmt.Errorf("attach: no connection to %v", remote)
	}
	client.sharedChannelsMu.RUnlock()

	client.opts.Logger.Debug("attach", zap.String("self", client.self.String()), zap.String("remote", remote.String()), zap.String("addr", conn.RemoteAddr().String()))
	if err := sc.ch.Attach(ctx, remote, conn, enc, dec); err != nil {
		return fmt.Errorf("attach: %v", err)
	}
	return nil
}

func (client *Client) Send(ctx context.Context, remote id.Signatory, msg wire.Msg) error {
	client.sharedChannelsMu.RLock()
	sc, ok := client.sharedChannels[remote]
	if !ok {
		client.sharedChannelsMu.RUnlock()
		return fmt.Errorf("send: no connection to %v", remote)
	}
	client.sharedChannelsMu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case sc.outbound <- msg:
		return nil
	}
}

func (client *Client) Receive(ctx context.Context, receiver chan<- Msg) {
	client.fanOutRunningMu.Lock()
	if client.fanOutRunning {
		return
	}
	client.fanOutRunning = true
	client.fanOutRunningMu.Unlock()

	go func() {
		fanOutReceivers := []fanOutReceiver{}

		for {
			select {
			case fanOutReceiver := <-client.fanOutReceivers:
				// A new fanOutReceiver has been registered.
				fanOutReceivers = append(fanOutReceivers, fanOutReceiver)
			case msg := <-client.inbound:
				marker := 0
				for _, fanOutReceiver := range fanOutReceivers {
					select {
					case <-fanOutReceiver.ctx.Done():
						// Do nothing. This will implicitly mark it for
						// deletion.
					case fanOutReceiver.receiver <- msg:
						fanOutReceivers[marker] = fanOutReceiver
						marker++
					}
				}
				// Delete everything that was marked for deletion.
				for del := marker; del < len(fanOutReceivers); del++ {
					fanOutReceivers[del] = fanOutReceiver{}
				}
				fanOutReceivers = fanOutReceivers[:marker]
			}
			if len(fanOutReceivers) == 0 {
				break
			}
		}

		client.fanOutRunningMu.Lock()
		client.fanOutRunning = false
		client.fanOutRunningMu.Unlock()
	}()

	select {
	case <-ctx.Done():
	case client.fanOutReceivers <- fanOutReceiver{ctx: ctx, receiver: receiver}:
	}
}
