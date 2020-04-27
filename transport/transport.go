package transport

import (
	"context"
	"fmt"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/listen"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

type Transport struct {
	opts Options

	tcpclient *tcp.Client
	tcpserver *tcp.Server

	pingListener listen.PingListener
	pushListener listen.PushListener
	pullListener listen.PullListener
}

func New(opts Options, handshaker handshake.Handshaker) *Transport {
	trans := &Transport{
		opts: opts,
	}
	trans.tcpclient = tcp.NewClient(opts.TCPClientOpts, handshaker, trans)
	trans.tcpserver = tcp.NewServer(opts.TCPServerOpts, handshaker, trans)
	return trans
}

func (trans *Transport) Run(ctx context.Context) {
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

				trans.opts.Logger.Infof("running")
				if err := trans.tcpserver.Listen(ctx); err != nil {
					trans.opts.Logger.Errorf("running: %v", err)
				}
			}()
		}
	}
}

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

func (trans *Transport) SetPingListener(listener listen.PingListener) {
	trans.pingListener = listener
}

func (trans *Transport) SetPushListener(listener listen.PushListener) {
	trans.pushListener = listener
}

func (trans *Transport) SetPullListener(listener listen.PullListener) {
	trans.pullListener = listener
}
