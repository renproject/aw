package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/experiment/channel"
	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/aw/experiment/policy"
	"github.com/renproject/aw/experiment/tcp"
	"github.com/renproject/aw/experiment/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Options struct {
	Logger          *zap.Logger
	Host            string
	Port            uint16
	Encoder         codec.Encoder
	Decoder         codec.Decoder
	OncePoolOptions handshake.OncePoolOptions
	ClientHandshake handshake.Handshake
	ServerHandshake handshake.Handshake
	ClientTimeout   time.Duration
	ServerTimeout   time.Duration
}

func DefaultOptions() Options {
	return Options{}
}

type Transport struct {
	opts Options

	self     id.Signatory
	client   channel.Client
	oncePool handshake.OncePool

	connectedMu *sync.RWMutex
	connected   map[id.Signatory]bool
}

func (t *Transport) Send(ctx context.Context, remote id.Signatory, remoteAddr string, msg wire.Msg) error {
	if t.IsConnected(remote) {
		return t.client.Send(ctx, remote, msg)
	}
	t.client.Bind(remote)
	go func() {
		defer t.client.Unbind(remote)
		t.dial(remote, remoteAddr)
	}()
	return t.client.Send(ctx, remote, msg)
}

func (t *Transport) Receive(ctx context.Context, receiver chan<- channel.Msg) {
	t.client.Receive(ctx, receiver)
}

func (t *Transport) Connect(remote id.Signatory, remoteAddr string) {
	t.connectedMu.Lock()
	_, isConnected := t.connected[remote]
	if !isConnected {
		t.connected[remote] = true
	}
	t.connectedMu.Lock()

	if !isConnected {
		t.client.Bind(remote)
		go t.dial(remote, remoteAddr)
	}
}

func (t *Transport) Disconnect(remote id.Signatory) {
	t.connectedMu.Lock()
	defer t.connectedMu.Lock()

	if t.connected[remote] {
		delete(t.connected, remote)
		t.client.Unbind(remote)
	}
}

func (t *Transport) IsConnected(remote id.Signatory) bool {
	t.connectedMu.RLock()
	defer t.connectedMu.RUnlock()

	_, ok := t.connected[remote]
	return ok
}

func (t *Transport) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			t.run(ctx)
		}
	}
}

func (t *Transport) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			t.opts.Logger.DPanic("recover", zap.Error(fmt.Errorf("%v", r)))
		}
	}()

	// Once handshakes are used to prevent multiple network connections between
	// the same peers. One network connection should be sufficient for all
	// communication.
	once := handshake.Once(t.self, &t.oncePool, t.opts.ServerHandshake)

	// Listen for incoming connection attempts.
	err := tcp.Listen(
		ctx,
		fmt.Sprintf("%v:%v", t.opts.Host, t.opts.Port),
		func(conn net.Conn) {
			// Capture the remote address while the connection is active so that
			// we can use it even after the connection dies.
			addr := conn.RemoteAddr().String()

			// Complete the "use once" handshake. This prevents multiple
			// connections being established with a remote peer if there is
			// already a (recent) existing one. This includes connected that we
			// have established by dialing the remote peer.
			enc, dec, remote, err := once(conn, t.opts.Encoder, t.opts.Decoder)
			if err != nil {
				t.opts.Logger.Error("handshake", zap.String("addr", addr), zap.Error(err))
				return
			}

			// Check if the Transport has explicitly connected to the remote
			// peer. If so, then we want this connection to be attached to a
			// Channel, and we want it to live for as long as the Transport
			// remains explicitly connected to the remote peer. As such, we use
			// the running context for the connection attachment.
			if t.IsConnected(remote) {
				if err := t.client.Attach(ctx, remote, conn, enc, dec); err != nil {
					t.opts.Logger.Error("incoming attachment", zap.String("remote", remote.String()), zap.String("addr", addr), zap.Error(err))
				}
				return
			}

			// Otherwise, this connection should be short-lived. A Channel still
			// needs to be created (because one probably does not exist), but a
			// bounded time should be used.
			ctx, cancel := context.WithTimeout(ctx, t.opts.ServerTimeout)
			defer cancel()

			t.client.Bind(remote)
			defer t.client.Unbind(remote)

			if err := t.client.Attach(ctx, remote, conn, enc, dec); err != nil {
				t.opts.Logger.Error("incoming attachment", zap.String("remote", remote.String()), zap.String("addr", addr), zap.Error(err))
			}
		},
		func(err error) {
			t.opts.Logger.Error("listen", zap.Error(err))
		},
		nil)
	if err != nil {
		t.opts.Logger.Error("listen", zap.Error(err))
	}
}

func (t *Transport) dial(remote id.Signatory, remoteAddr string) {
	for {
		func() {
			var innerCtx context.Context
			var innerCancel context.CancelFunc
			if t.IsConnected(remote) {
				innerCtx, innerCancel = context.WithCancel(context.Background())
			} else {
				innerCtx, innerCancel = context.WithTimeout(context.Background(), t.opts.ClientTimeout)
			}
			defer innerCancel()

			err := tcp.Dial(
				innerCtx,
				remoteAddr,
				func(conn net.Conn) {
					addr := conn.RemoteAddr().String()

					once := handshake.Once(t.self, &t.oncePool, t.opts.ClientHandshake)
					enc, dec, r, err := once(conn, t.opts.Encoder, t.opts.Decoder)
					if r != remote {
						t.opts.Logger.Error("handshake", zap.String("expected", remote.String()), zap.String("got", r.String()), zap.Error(fmt.Errorf("bad remote")))
					}
					if err != nil {
						t.opts.Logger.Error("handshake", zap.String("remote", remote.String()), zap.String("addr", addr), zap.Error(err))
						return
					}

					if err := t.client.Attach(innerCtx, remote, conn, enc, dec); err != nil {
						t.opts.Logger.Error("outgoing attach", zap.String("remote", remote.String()), zap.String("addr", addr), zap.Error(err))
					}
				},
				func(err error) {
					t.opts.Logger.Error("dial", zap.String("remote", remote.String()), zap.String("addr", remoteAddr), zap.Error(err))
				},
				policy.ConstantTimeout(t.opts.ClientTimeout))
			if err != nil {
				t.opts.Logger.Error("dial", zap.String("remote", remote.String()), zap.String("addr", remoteAddr), zap.Error(err))
			}
		}()
		if !t.IsConnected(remote) {
			return
		}
	}
}
