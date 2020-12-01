package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/dht"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/policy"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

// Default options.
var (
	DefaultHost          = "localhost"
	DefaultPort          = uint16(3333)
	DefaultEncoder       = codec.PlainEncoder
	DefaultDecoder       = codec.PlainDecoder
	DefaultDialTimeout   = policy.ConstantTimeout(time.Second)
	DefaultClientTimeout = 10 * time.Second
	DefaultServerTimeout = 10 * time.Second
)

// Options used to parameterise the behaviour of a Transport.
type Options struct {
	Logger          *zap.Logger
	Host            string
	Port            uint16
	Encoder         codec.Encoder
	Decoder         codec.Decoder
	DialTimeout     policy.Timeout
	ClientTimeout   time.Duration
	ServerTimeout   time.Duration
	OncePoolOptions handshake.OncePoolOptions
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return Options{
		Logger:          logger,
		Host:            DefaultHost,
		Port:            DefaultPort,
		Encoder:         DefaultEncoder,
		Decoder:         DefaultDecoder,
		DialTimeout:     DefaultDialTimeout,
		ClientTimeout:   DefaultClientTimeout,
		ServerTimeout:   DefaultServerTimeout,
		OncePoolOptions: handshake.DefaultOncePoolOptions(),
	}
}

func (opts Options) WithLogger(logger *zap.Logger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithPort(port uint16) Options {
	opts.Port = port
	return opts
}

func (opts Options) WithClientTimeout(timeout time.Duration) Options {
	opts.ClientTimeout = timeout
	return opts
}

func (opts Options) WithServerTimeout(timeout time.Duration) Options {
	opts.ServerTimeout = timeout
	return opts
}

func (opts Options) WithOncePoolOptions(oncePoolOpts handshake.OncePoolOptions) Options {
	opts.OncePoolOptions = oncePoolOpts
	return opts
}

type Transport struct {
	opts Options

	self   id.Signatory
	client *channel.Client
	once   handshake.Handshake

	linksMu *sync.RWMutex
	links   map[id.Signatory]bool

	connsMu *sync.RWMutex
	conns   map[id.Signatory]int64

	table dht.Table
}

func New(opts Options, self id.Signatory, client *channel.Client, h handshake.Handshake, table dht.Table) *Transport {
	oncePool := handshake.NewOncePool(opts.OncePoolOptions)
	return &Transport{
		opts: opts,

		self:   self,
		client: client,
		once:   handshake.Once(self, &oncePool, h),

		linksMu: new(sync.RWMutex),
		links:   map[id.Signatory]bool{},

		connsMu: new(sync.RWMutex),
		conns:   map[id.Signatory]int64{},

		table: table,
	}
}

func (t *Transport) Table() dht.Table {
	return t.table
}

func (t *Transport) Send(ctx context.Context, remote id.Signatory, msg wire.Msg) error {
	remoteAddr, ok := t.table.PeerAddress(remote)
	if !ok {
		return fmt.Errorf("peer not found: %v", remote)
	}

	if t.IsConnected(remote) {
		t.opts.Logger.Debug("send", zap.Bool("connected", true), zap.String("remote", remote.String()), zap.String("addr", remoteAddr))
		return t.client.Send(ctx, remote, msg)
	}

	if t.IsLinked(remote) {
		t.opts.Logger.Debug("send", zap.Bool("linked", true), zap.String("remote", remote.String()), zap.String("addr", remoteAddr))
		go t.dial(ctx, remote, remoteAddr)
		return t.client.Send(ctx, remote, msg)
	}

	t.opts.Logger.Debug("send", zap.Bool("linked", false), zap.Bool("connected", false), zap.String("remote", remote.String()), zap.String("addr", remoteAddr))
	t.client.Bind(remote)
	go func() {
		defer t.client.Unbind(remote)
		t.dial(ctx, remote, remoteAddr)
	}()
	return t.client.Send(ctx, remote, msg)
}

func (t *Transport) Receive(ctx context.Context, receiver func(id.Signatory, wire.Msg) error) {
	t.client.Receive(ctx, receiver)
}

func (t *Transport) Link(remote id.Signatory) {
	t.linksMu.Lock()
	defer t.linksMu.Unlock()

	if t.links[remote] {
		return
	}
	t.client.Bind(remote)
	t.links[remote] = true
}

func (t *Transport) Unlink(remote id.Signatory) {
	t.linksMu.Lock()
	defer t.linksMu.Lock()

	if t.links[remote] {
		t.client.Unbind(remote)
		delete(t.links, remote)
	}
}

func (t *Transport) IsLinked(remote id.Signatory) bool {
	t.linksMu.Lock()
	defer t.linksMu.Unlock()

	return t.links[remote]
}

func (t *Transport) IsConnected(remote id.Signatory) bool {
	t.connsMu.RLock()
	defer t.connsMu.RUnlock()

	return t.conns[remote] > 0
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

	// Listen for incoming connection attempts.
	t.opts.Logger.Info("listening", zap.String("host", t.opts.Host), zap.Uint16("port", t.opts.Port))
	err := tcp.Listen(
		ctx,
		fmt.Sprintf("%v:%v", t.opts.Host, t.opts.Port),
		func(conn net.Conn) {
			addr := conn.RemoteAddr().String()
			enc, dec, remote, err := t.once(conn, t.opts.Encoder, t.opts.Decoder)
			if err != nil {
				t.opts.Logger.Error("handshake", zap.String("addr", addr), zap.Error(err))
				return
			}

			t.table.AddPeer(remote, addr)
			enc = codec.LengthPrefixEncoder(codec.PlainEncoder, enc)
			dec = codec.LengthPrefixDecoder(codec.PlainDecoder, dec)

			t.connect(remote)
			defer t.disconnect(remote)

			// If the Transport is linked to the remote peer, then the
			// network connection should be kept alive until the remote peer
			// is unlinked (or the network connection faults).
			if t.IsLinked(remote) {
				t.opts.Logger.Debug("accepted", zap.Bool("linked", true), zap.String("remote", remote.String()), zap.String("addr", addr))
				defer t.opts.Logger.Debug("accepted: drop", zap.Bool("linked", true), zap.String("remote", remote.String()), zap.String("addr", addr))

				// Attaching a connection will block until the Channel is
				// unbound (which happens when the Transport is unlinked), the
				// connection is replaced, or the connection faults.
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

			t.opts.Logger.Debug("accepted", zap.Bool("linked", false), zap.Duration("timeout", t.opts.ServerTimeout), zap.String("remote", remote.String()), zap.String("addr", addr))
			defer t.opts.Logger.Debug("accepted: drop", zap.Bool("linked", false), zap.Duration("timeout", t.opts.ServerTimeout), zap.String("remote", remote.String()), zap.String("addr", addr))

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

func (t *Transport) dial(retryCtx context.Context, remote id.Signatory, remoteAddr string) {
	// It is tempting to skip dialing if there is already a connection. However,
	// it is desirable to be able to re-dial in the case that the network
	// address has changed. As such, we do not do any skip checks, and assume
	// that dial is only called when the caller is absolutely sure that a dial
	// should happen.

	for {
		dialCtx, cancel := context.WithTimeout(context.Background(), t.opts.ClientTimeout)
		defer cancel()

		t.opts.Logger.Debug("dialing", zap.String("remote", remote.String()), zap.String("addr", remoteAddr))

		err := tcp.Dial(
			dialCtx,
			remoteAddr,
			func(conn net.Conn) {
				addr := conn.RemoteAddr().String()
				enc, dec, r, err := t.once(conn, t.opts.Encoder, t.opts.Decoder)
				if err != nil {
					t.opts.Logger.Error("handshake", zap.String("remote", remote.String()), zap.String("addr", addr), zap.Error(err))
					return
				}
				if !r.Equal(&remote) {
					t.opts.Logger.Error("handshake", zap.String("expected", remote.String()), zap.String("got", r.String()), zap.Error(fmt.Errorf("bad remote")))
				}

				enc = codec.LengthPrefixEncoder(codec.PlainEncoder, enc)
				dec = codec.LengthPrefixDecoder(codec.PlainDecoder, dec)

				t.connect(remote)
				defer t.disconnect(remote)

				if t.IsLinked(remote) {
					t.opts.Logger.Debug("dialed", zap.Bool("linked", true), zap.String("remote", remote.String()), zap.String("addr", addr))
					defer t.opts.Logger.Debug("dialed: drop", zap.Bool("linked", true), zap.String("remote", remote.String()), zap.String("addr", addr))

					// If the Transport is linked to the remote peer, then the
					// network connection should be kept alive until the remote peer
					// is unlinked (or the network connection faults). To do this,
					// we override the context and re-use it. Otherwise, the
					// previously defined context will be used, which will
					// eventually timeout.
					dialCtx = context.Background()
				} else {
					t.opts.Logger.Debug("dialed", zap.Bool("linked", false), zap.Duration("timeout", t.opts.ClientTimeout), zap.String("remote", remote.String()), zap.String("addr", addr))
					defer t.opts.Logger.Debug("dialed: drop", zap.Bool("linked", false), zap.Duration("timeout", t.opts.ClientTimeout), zap.String("remote", remote.String()), zap.String("addr", addr))
				}

				if err := t.client.Attach(dialCtx, remote, conn, enc, dec); err != nil {
					t.opts.Logger.Error("outgoing", zap.String("remote", remote.String()), zap.String("addr", addr), zap.Error(err))
				}
			},
			func(err error) {
				t.opts.Logger.Error("dial", zap.String("remote", remote.String()), zap.String("addr", remoteAddr), zap.Error(err))
			},
			t.opts.DialTimeout)
		if err != nil {
			t.opts.Logger.Error("dial", zap.String("remote", remote.String()), zap.String("addr", remoteAddr), zap.Error(err))
			select {
			case <-retryCtx.Done():
			case <-dialCtx.Done():
				if !t.IsConnected(remote) {
					continue
				}
			}
		}
		return
	}
}

func (t *Transport) connect(remote id.Signatory) {
	t.connsMu.Lock()
	defer t.connsMu.Unlock()

	t.conns[remote]++
}

func (t *Transport) disconnect(remote id.Signatory) {
	t.connsMu.Lock()
	defer t.connsMu.Unlock()

	if t.conns[remote] > 0 {
		if t.conns[remote]--; t.conns[remote] == 0 {
			delete(t.conns, remote)
		}
	}
}
