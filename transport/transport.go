package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

type Transport struct {
	opts Options

	isRunningMu *sync.Mutex
	isRunning   bool

	tcpclient *tcp.Client
	tcpserver *tcp.Server

	pingListener wire.PingListener
	pushListener wire.PushListener
	pullListener wire.PullListener
}

func New(opts Options, handshaker handshake.Handshaker) *Transport {
	trans := &Transport{
		opts: opts,

		isRunningMu: new(sync.Mutex),
		isRunning:   false,
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
			func() {
				defer func() {
					if r := recover(); r != nil {
						trans.opts.Logger.Errorf("recovering: %v", r)
					}
				}()

				if err := trans.tcpserver.Listen(ctx); err != nil {
					trans.opts.Logger.Errorf("%v", err)
				}
			}()
		}
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

// SetPingListener must be called before calling the Run method. Otherwise, this
// method will panic.
func (trans *Transport) SetPingListener(listener wire.PingListener) {
	trans.isRunningMu.Lock()
	defer trans.isRunningMu.Unlock()

	if trans.isRunning {
		panic("transport is already running")
	}
	trans.pingListener = listener
}

// SetPushListener must be called before calling the Run method. Otherwise, this
// method will panic.
func (trans *Transport) SetPushListener(listener wire.PushListener) {
	trans.isRunningMu.Lock()
	defer trans.isRunningMu.Unlock()

	if trans.isRunning {
		panic("transport is already running")
	}
	trans.pushListener = listener
}

// SetPullListener must be called before calling the Run method. Otherwise, this
// method will panic.
func (trans *Transport) SetPullListener(listener wire.PullListener) {
	trans.isRunningMu.Lock()
	defer trans.isRunningMu.Unlock()

	if trans.isRunning {
		panic("transport is already running")
	}
	trans.pullListener = listener
}

func (trans *Transport) DidReceivePing(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if trans.pingListener != nil {
		return trans.pingListener.DidReceivePing(version, data, from)
	}
	return wire.Message{Version: version, Type: wire.PingAck, Data: []byte{}}, nil
}

func (trans *Transport) DidReceivePingAck(version uint8, data []byte, from id.Signatory) error {
	if trans.pingListener != nil {
		return trans.pingListener.DidReceivePingAck(version, data, from)
	}
	return nil
}

func (trans *Transport) DidReceivePush(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if trans.pushListener != nil {
		return trans.pushListener.DidReceivePush(version, data, from)
	}
	return wire.Message{Version: version, Type: wire.PushAck, Data: []byte{}}, nil
}

func (trans *Transport) DidReceivePushAck(version uint8, data []byte, from id.Signatory) error {
	if trans.pushListener != nil {
		return trans.pushListener.DidReceivePushAck(version, data, from)
	}
	return nil
}

func (trans *Transport) DidReceivePull(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
	if trans.pullListener != nil {
		return trans.pullListener.DidReceivePull(version, data, from)
	}
	return wire.Message{Version: version, Type: wire.PullAck, Data: []byte{}}, nil
}

func (trans *Transport) DidReceivePullAck(version uint8, data []byte, from id.Signatory) error {
	if trans.pullListener != nil {
		return trans.pullListener.DidReceivePullAck(version, data, from)
	}
	return nil
}
