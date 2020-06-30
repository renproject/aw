package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Transport struct {
	opts Options

	isRunningMu *sync.Mutex
	isRunning   bool

	tcpclient *tcp.Client
	tcpserver *tcp.Server

	pingListeners []wire.PingListener
	pushListeners []wire.PushListener
	pullListeners []wire.PullListener
}

func New(opts Options, handshaker handshake.Handshaker) *Transport {
	trans := &Transport{
		opts: opts,

		isRunningMu: new(sync.Mutex),
		isRunning:   false,

		pingListeners: []wire.PingListener{},
		pushListeners: []wire.PushListener{},
		pullListeners: []wire.PullListener{},
	}
	trans.tcpclient = tcp.NewClient(opts.TCPClientOpts, handshaker, trans)
	trans.tcpserver = tcp.NewServer(opts.TCPServerOpts, handshaker, trans)
	return trans
}

// Run the Transport until the context is done. This method must only be called
// after the SetPingListener, SetPushListener, and SetPullListener methods have
// been called (if necessary). Otherwise, this method will panic.
func (trans *Transport) Run(ctx context.Context) {
	trans.isRunningMu.Lock()
	isAlreadyRunning := trans.isRunning
	trans.isRunning = true
	trans.isRunningMu.Unlock()

	if isAlreadyRunning {
		panic("transport is already running")
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			trans.run(ctx)
		}
	}
}

func (trans *Transport) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			trans.opts.Logger.Errorf("recovering: %v", r)
		}
	}()
	if err := trans.tcpserver.Listen(ctx); err != nil {
		trans.opts.Logger.Errorf("%v", err)
	}
}

// Send a message to an address. The Transport will select the appropriate
// client implementation based on the protocol in the target address. If the
// protocol is not supported, an error is returned. Otherwise, any error from
// the selected client is returned.
func (trans *Transport) Send(ctx context.Context, addr wire.Address, msg wire.Message) error {
	switch addr.Protocol {
	case wire.TCP:
		return trans.tcpclient.Send(ctx, addr.Value, msg)
	case wire.UDP:
		return fmt.Errorf("unsupported protocol=udp")
	case wire.WebSocket:
		return fmt.Errorf("unsupported protocol=ws")
	default:
		return fmt.Errorf("unsupported protocol=%v", addr.Protocol)
	}
}

// ListenForPings must be called before calling the Run method. Otherwise, this
// method will panic.
func (trans *Transport) ListenForPings(listener wire.PingListener) {
	trans.isRunningMu.Lock()
	defer trans.isRunningMu.Unlock()

	if trans.isRunning {
		panic("transport is already running")
	}
	trans.pingListeners = append(trans.pingListeners, listener)
}

// ListenForPushes must be called before calling the Run method. Otherwise, this
// method will panic.
func (trans *Transport) ListenForPushes(listener wire.PushListener) {
	trans.isRunningMu.Lock()
	defer trans.isRunningMu.Unlock()

	if trans.isRunning {
		panic("transport is already running")
	}
	trans.pushListeners = append(trans.pushListeners, listener)
}

// ListenForPulls must be called before calling the Run method. Otherwise, this
// method will panic.
func (trans *Transport) ListenForPulls(listener wire.PullListener) {
	trans.isRunningMu.Lock()
	defer trans.isRunningMu.Unlock()

	if trans.isRunning {
		panic("transport is already running")
	}
	trans.pullListeners = append(trans.pullListeners, listener)
}

// DidReceivePing is a callback that is invoked when a ping is received by the
// transport.
func (trans *Transport) DidReceivePing(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	for _, listener := range trans.pingListeners {
		// TODO: We need (a) aggregate multiple response into a single response,
		// (b) make the request/response cycle asynchronous and explicitly
		// support multiple responses, or (c) continue with this pattern.
		response, err := listener.DidReceivePing(version, data, from)
		if err != nil {
			trans.opts.TCPServerOpts.Logger.Error("receiving ping", zap.Error(err))
			continue
		}
		return response, nil
	}
	return wire.Message{Version: version, Type: wire.PingAck, Data: []byte{}}, nil
}

// DidReceivePingAck is a callback that is invoked when a ping acknowledgement
// is received by the transport.
func (trans *Transport) DidReceivePingAck(version uint8, data []byte, from id.Signatory) error {
	for _, listener := range trans.pingListeners {
		// TODO: We need (a) aggregate multiple response into a single response,
		// (b) make the request/response cycle asynchronous and explicitly
		// support multiple responses, or (c) continue with this pattern.
		if err := listener.DidReceivePingAck(version, data, from); err != nil {
			trans.opts.TCPServerOpts.Logger.Error("receiving ping ack", zap.Error(err))
		}
	}
	return nil
}

// DidReceivePush is a callback that is invoked when a push is received by the
// transport.
func (trans *Transport) DidReceivePush(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	for _, listener := range trans.pushListeners {
		// TODO: We need (a) aggregate multiple response into a single response,
		// (b) make the request/response cycle asynchronous and explicitly
		// support multiple responses, or (c) continue with this pattern.
		response, err := listener.DidReceivePush(version, data, from)
		if err != nil {
			trans.opts.TCPServerOpts.Logger.Error("receiving push", zap.Error(err))
			continue
		}
		return response, nil
	}
	return wire.Message{Version: version, Type: wire.PushAck, Data: []byte{}}, nil
}

// DidReceivePushAck is a callback that is invoked when a push acknowledgement
// is received by the transport.
func (trans *Transport) DidReceivePushAck(version uint8, data []byte, from id.Signatory) error {
	for _, listener := range trans.pushListeners {
		// TODO: We need (a) aggregate multiple response into a single response,
		// (b) make the request/response cycle asynchronous and explicitly
		// support multiple responses, or (c) continue with this pattern.
		if err := listener.DidReceivePushAck(version, data, from); err != nil {
			trans.opts.TCPServerOpts.Logger.Error("receiving push ack", zap.Error(err))
		}
	}
	return nil
}

// DidReceivePull is a callback that is invoked when a pull is received by the
// transport.
func (trans *Transport) DidReceivePull(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	for _, listener := range trans.pullListeners {
		// TODO: We need (a) aggregate multiple response into a single response,
		// (b) make the request/response cycle asynchronous and explicitly
		// support multiple responses, or (c) continue with this pattern.
		response, err := listener.DidReceivePull(version, data, from)
		if err != nil {
			trans.opts.TCPServerOpts.Logger.Error("receiving pull", zap.Error(err))
			continue
		}
		return response, nil
	}
	return wire.Message{Version: version, Type: wire.PullAck, Data: []byte{}}, nil
}

// DidReceivePullAck is a callback that is invoked when a pull acknowledgement
// is received by the transport.
func (trans *Transport) DidReceivePullAck(version uint8, data []byte, from id.Signatory) error {
	for _, listener := range trans.pullListeners {
		// TODO: We need (a) aggregate multiple response into a single response,
		// (b) make the request/response cycle asynchronous and explicitly
		// support multiple responses, or (c) continue with this pattern.
		if err := listener.DidReceivePullAck(version, data, from); err != nil {
			trans.opts.TCPServerOpts.Logger.Error("receiving pull ack", zap.Error(err))
		}
	}
	return nil
}
