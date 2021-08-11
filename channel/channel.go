package channel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// An Attacher is able to attach a network connection to itself.
type Attacher interface {
	Attach(ctx context.Context, remote id.Signatory, conn net.Conn, encoder codec.Encoder, decoder codec.Decoder) error
}

// Default options.
var ()

// reader represents the read-half of a network connection. It also contains a
// quit channel that is closed when the reader is no longer being used by the
// Channel.
type reader struct {
	net.Conn
	io.Reader
	codec.Decoder

	// q is a quit channel that is closed by the Channel when the reader is no
	// longer being used. This happens when the network connection faults, or is
	// replaced by a new network connection.
	q chan<- struct{}
}

// writer represents the write-half of a network connection. It also contains a
// quit channel that is closed when the writer is no longer being used by the
// Channel.
type writer struct {
	net.Conn
	*bufio.Writer
	codec.Encoder

	// q is a quit channel that is closed by the Channel when the writer is no
	// longer being used. This happens when the network connection faults, or is
	// replaced by a new network connection.
	q chan<- struct{}
}

// A Channel is an abstraction over a network connection. It can be created
// independently of a network connection, it can have network connections
// attached and detached, it can replace its network connection, and it persists
// when the network connection faults. Channels are connected to remote peers,
// not network addresses, which also allows Channels to be agnostic to the
// network addresses. If the network address of the remote peer changes, a new
// network connection can be dialed, and then attached (replacing any existing
// attachment).
//
// Channels are designed for sending messages between remote peers over a
// network connection, where the network connection might fault, or be replaced.
// However, unlike a network connection, a Channel does not implement IO
// reader/writer interfaces. Instead, messages are sent/received to/from a
// Channel using externalized outbound/inbound messaging channels.
//
//	// Make some messaging channels that we can use to interact with our
//	// networking Channel.
//	inbound, outbound := make(chan wire.Msg, 1), make(chan wire.Msg)
//	// Create a networking Channel that will read from the inbound messaging
//	// channel.
//	ch := channel.New(remote, inbound, outbound)
//	// Run the Channel so that it can process the inbound and outbound message
//	// channels.
//	go ch.Run(ctx)
//	// Read inbound messages that have been sent by the remote peer and echo
//	// them back to the remote peer.
//	for Msg := range inbound {
//		outbound <- Msg
//	}
//	// Attach a network connection to the remote peer.
//	// ...
//
// A Channel must be explicitly Run (see the Run method) before it will begin
// processing the outbound/inbound messaging channels. Messages on outbound
// messaging channel are read by the Channel and then written to the currently
// attached network connection (or persisted until a network connection is
// attached). Similarly, whenever there is an attached network connection, the
// Channel reads messages from the network connection and writes them to the
// inbound messaging channel. Channels are safe for concurrent use.
type Channel struct {
	opts   Options
	remote id.Signatory

	inbound  chan<- wire.Packet
	outbound <-chan wire.Msg

	readers chan reader
	writers chan writer

	rateLimiter *rate.Limiter
}

// New returns an abstract Channel connection to a remote peer. It will have no
// network connection attached. The Channel will write messages from attached
// network connections to the inbound messaging channel, and will write messages
// from the outbound messaging channel to attached network connections.
//
// The back-pressure that the Channel endure depends on the capacity of the
// inbound and outbound messaging channels; more capacity allows for more
// back-pressure. Back-pressure builds when messages are being written to the
// outbound messaging channel, but there is no functional attached network
// connection, or when messages are being received on an attached network
// connection, but the inbound message channel is not being drained.
func New(opts Options, remote id.Signatory, inbound chan<- wire.Packet, outbound <-chan wire.Msg) *Channel {
	return &Channel{
		opts:   opts,
		remote: remote,

		inbound:  inbound,
		outbound: outbound,

		readers: make(chan reader, 1),
		writers: make(chan writer, 1),

		rateLimiter: rate.NewLimiter(opts.RateLimit, opts.MaxMessageSize),
	}
}

// Run the read/write loops until the context is done, or an error is
// encountered. Channels should be Run before attaching network connections,
// sending messages to the outbound messaging channel, or receiving messages
// from the inbound messaging channel.
//
// Attaching a new network connection to a Channel will not interrupt it.
// Messages that have been received (regardless of changes to the attached
// network connection) will always eventually be written to the inbound
// messaging channel. Similarly, messages that are on the outbound queue will
// always eventually be written to at least one attached network connection.
func (ch *Channel) Run(ctx context.Context) error {
	go ch.writeLoop(ctx)
	return ch.readLoop(ctx)
}

// Attach a network connection to the Channel. This will replace the existing
// network connection used by the Channel for reading/writing inbound/outbound
// messages. If the Channel is not running, this method will block until the
// Channel is run, or the context is done. Otheriwse, it blocks until the now
// attached network connection faults, is replaced, the Channel stops running,
// or the context is done.
//
//	// Create and run a channel.
//	ch := channel.New(remote, inbound, outbound)
//	go ch.Run(ctx)
//
//	// Dial a new connection and attach it to the Channel. Now, writing to the
//	// outbound messaging channel will send messagse to the peer at the other
//	// end of the dialed connection (and vice versa for the inbound messaging
//	// channel).
//	tcp.Dial(
//		ctx,
//		remoteAddr,
//		func(conn net.Conn) {
//			conn, enc, dec, err := handshake.Insecure(conn)
//			if err == nil {
//				ch.Attach(ctx, conn, enc, dec)
//			}
//		},
//		nil,
//		nil)
//
func (ch *Channel) Attach(ctx context.Context, remote id.Signatory, conn net.Conn, enc codec.Encoder, dec codec.Decoder) error {
	if !ch.remote.Equal(&remote) {
		return fmt.Errorf("bad remote: expected %v, got %v", ch.remote, remote)
	}

	rq := make(chan struct{})
	wq := make(chan struct{})

	// Signal that a new reader should be used.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch.readers <- reader{Conn: conn, Reader: bufio.NewReaderSize(conn, ch.opts.MaxMessageSize), Decoder: dec, q: rq}:
	}
	// Signal that a new writer should be used.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch.writers <- writer{Conn: conn, Writer: bufio.NewWriterSize(conn, ch.opts.MaxMessageSize), Encoder: enc, q: wq}:
	}

	// Wait for the reader to be closed.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rq:
	}
	// Wait for the writer to be closed.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wq:
	}

	return nil
}

// Remote peer identity expected by the Channel.
func (ch Channel) Remote() id.Signatory {
	return ch.remote
}

func (ch *Channel) readLoop(ctx context.Context) error {
	read := func(r reader, drain <-chan struct{}) {
		draining := uint64(0)

		// If the drain channel is written to, this signals that this reader is
		// now expired and we should begin draining it.
		go func() {
			<-drain

			atomic.StoreUint64(&draining, 1)

			// Set the read deadline here, instead of per-message, so that the
			// remote peer cannot easily "slow loris" the local peer by
			// periodically sending messages into the draining connection.
			if err := r.Conn.SetReadDeadline(time.Now().Add(ch.opts.DrainTimeout)); err != nil {
				ch.opts.Logger.Error("drain: set deadline", zap.Error(err))
				return
			}
		}()

		buf := make([]byte, ch.opts.MaxMessageSize)
		bufSyncData := make([]byte, ch.opts.MaxMessageSize)

		for {
			n, err := r.Decoder(r.Reader, buf[:])
			if err != nil {
				draining := atomic.LoadUint64(&draining)

				// If the reader is closed, we don't print the error message
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
					ch.opts.Logger.Error("decode", zap.Uint64("draining", draining), zap.Error(err))
				}
				close(r.q)
				return
			}

			// Check that the underlying connection is not exceeding its rate
			// limit.
			if !ch.rateLimiter.AllowN(time.Now(), n) {
				ch.opts.Logger.Error("rate limit exceeded", zap.String("remote", ch.remote.String()), zap.String("addr", r.Conn.RemoteAddr().String()))
				close(r.q)
				return
			}

			m := wire.Msg{}

			// Unmarshal the message from binary. If this is successfully, then
			// we mark the message as available (and will attempt to write it to
			// the inbound message channel).
			if _, _, err := m.Unmarshal(buf[:n], len(buf)); err != nil {
				ch.opts.Logger.Error("unmarshal", zap.Error(err))
				continue
			}

			// An aggressive filtering strategy would involve pre-filtering
			// synchronisation messages before reading the synchronisation data.
			// However, in practice, this does not provide much of an advantage
			// because we can (a) restrict how much synchronisation data we are
			// willing to read upfront and count it against bandwidth
			// rate-limiting, and (b) filtering that happens in the client
			// results in bad channels being killed quickly anyway.
			if m.Type == wire.MsgTypeSync {
				n, err := r.Decoder(r.Reader, bufSyncData)
				if err != nil {
					ch.opts.Logger.Error("decode sync data", zap.Error(err))
					// If reading from the reader fails, then clear the reader. This
					// will cause the next iteration to wait until a new underlying
					// network connection is attached to the Channel.
					close(r.q)
					return
				}
				m.SyncData = make([]byte, n)
				copy(m.SyncData, bufSyncData[:n])
			}

			select {
			case <-ctx.Done():
				if r.q != nil {
					close(r.q)
				}
				return
			case ch.inbound <- wire.Packet{Msg: m, IPAddr: r.Conn.RemoteAddr()}:
			}
		}
	}

	drain := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			close(drain)
			return ctx.Err()
		case r := <-ch.readers:
			ch.opts.Logger.Debug("replaced reader", zap.String("remote", ch.remote.String()), zap.String("addr", r.Conn.RemoteAddr().String()))

			drain <- struct{}{}            // Write to the previous drain channel.
			drain = make(chan struct{}, 1) // Create a new drain channel.
			go read(r, drain)
		}
	}
}

func (ch *Channel) writeLoop(ctx context.Context) {
	buf := make([]byte, ch.opts.MaxMessageSize)

	var w writer
	var wOk bool

	var m wire.Msg
	var mOk bool
	var mQueue <-chan wire.Msg

	for {
		switch {
		case wOk && mOk:
			q := make(chan wire.Msg, 1)
			q <- m
			mQueue = q
		case wOk:
			mQueue = ch.outbound
		default:
			mQueue = nil
		}

		select {
		case <-ctx.Done():
			if w.q != nil {
				close(w.q)
			}
			return
		case v, vOk := <-ch.writers:
			if w.q != nil {
				close(w.q)
			}
			w, wOk = v, vOk
		case m, mOk = <-mQueue:
			tail, _, err := m.Marshal(buf[:], len(buf))
			if err != nil {
				ch.opts.Logger.Error("marshal", zap.Error(err))
				// Clear the latest message so that we can move on to other
				// messages. We do this, because failure to marshal is not
				// something that is typically recoverable.
				m = wire.Msg{}
				mOk = false
				continue
			}
			if _, err := w.Encoder(w.Writer, buf[:len(buf)-len(tail)]); err != nil {
				ch.opts.Logger.Error("encode", zap.Error(err))
				// If an error happened when trying to write to the writer,
				// then clean the writer. This will force the Channel to
				// block on future writes until a new network connection is
				// attached. The latest message is not replaced (so we will
				// re-attempt to write it when a new connection is
				// eventually attached).
				close(w.q)
				w, wOk = writer{}, false
				continue
			}
			if err := w.Writer.Flush(); err != nil {
				// syscall.EPIPE is returned when the pipeline is broken which
				// mean the connection has been closed.
				if !errors.Is(err, syscall.EPIPE) {
					ch.opts.Logger.Error("flush", zap.Error(err))
				}
				// An error when flushing is the same as an error when encoding.
				close(w.q)
				w, wOk = writer{}, false
				continue
			}
			if m.Type == wire.MsgTypeSync {
				if _, err := w.Encoder(w.Writer, m.SyncData); err != nil {
					ch.opts.Logger.Error("encode", zap.NamedError("sync data", err))
					close(w.q)
					w, wOk = writer{}, false
					continue
				}
				if err := w.Writer.Flush(); err != nil {
					if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
						ch.opts.Logger.Error("flush", zap.NamedError("sync data", err))
					}
					// An error when flushing is the same as an error when encoding.
					close(w.q)
					w, wOk = writer{}, false
					continue
				}
			}

			// Clear the latest message so that we can move on to other
			// messages.
			m = wire.Msg{}
			mOk = false
		}
	}
}
