package channel

import (
	"context"
	"net"

	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/aw/experiment/wire"
	"github.com/renproject/id"
)

type reader struct {
	net.Conn
	codec.Decoder

	q chan<- struct{}
}

type writer struct {
	net.Conn
	codec.Encoder

	q chan<- struct{}
}

// A Channel is an abstraction over a network connection that is persistent,
// even when the underlying network connection is faulty. A Channel uses
// inbound/outbound message queues to persist messages that are being
// sent/received to the underlying network connection. If the connection fails,
// or is replaced, these queues persist and the messages are not lost.
//
// In contrast to a Channel, network connections are also bound to a remote
// network address, and if the network address changes, the connection needs to
// be closed and re-opened. Instead, a Channel is bound to a remote peer ID,
// and is agnostic to the actual network address. As network addresses change,
// and new connections are opened, they can be attached to the Channel. The
// Channel will then begin using this new connection without losing any of the
// messages in its queues.
//
// Channels must be running before connections can be attached, or messages
// sent/received to/from its messaging queues. Otherwise, these actions will
// block.
//
// Channels are safe for concurrent use.
type Channel struct {
	self   id.Signatory
	remote id.Signatory

	inbound  chan<- wire.Msg
	outbound <-chan wire.Msg

	readers chan reader
	writers chan writer
}

// New returns a Channel between the local and remote peer. It will write
// messagse to the inbound message queue that is reads from the network
// connection, and read messages from the outbound message queue to write them
// to the network connection.
func New(self, remote id.Signatory, inbound chan<- wire.Msg, outbound <-chan wire.Msg) *Channel {
	return &Channel{
		self:   self,
		remote: remote,

		inbound:  inbound,
		outbound: outbound,

		readers: make(chan reader, 1),
		writers: make(chan writer, 1),
	}
}

// Run the read/write loops until the context is done, or an error is
// encountered. Channels should be run before attaching connections, sending
// messages to the outbound queue, or receiving message from the inbound queue.
// Otherwise, these actions will block.
//
// Attaching a new connection to a Channel will not interrupt its running state.
// Messages that are received will always eventually be written to the inbound
// queue, and messages that are on the outbound queue will always eventually be
// written to at least one connection.
func (ch *Channel) Run(ctx context.Context) error {
	go ch.writeLoop(ctx)
	return ch.readLoop(ctx)
}

// Attach a network connection to the Channel. This will replace the existing
// network connection used by the Channel for reading/writing inbound/outbound
// messages. If the Channel is not running, this method will block until the
// Channel is run, or until the context is done. Otheriwse, it blocks until the
// now attacked network connection faults, is replaced, or the context is done.
func (ch *Channel) Attach(ctx context.Context, conn net.Conn, enc codec.Encoder, dec codec.Decoder) error {
	rq := make(chan struct{})
	wq := make(chan struct{})

	// Signal that a new reader should be used.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch.readers <- reader{Conn: conn, Decoder: dec, q: rq}:
	}
	// Signal that a new writer should be used.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch.writers <- writer{Conn: conn, Encoder: enc, q: wq}:
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

// Self returns the local peer ID that was established during the handshake for
// the underlying network connection.
func (ch Channel) Self() id.Signatory {
	return ch.self
}

// Remote returns the remote peer ID that was verified during the handshake for
// the underlying network connection.
func (ch Channel) Remote() id.Signatory {
	return ch.remote
}

func (ch Channel) readLoop(ctx context.Context) error {
	var buf [4194304]byte // TODO: Make this configurable. Currently hard-coded to be 4MB.

	// Currently active network connection from which we are decoding messages
	// to be written to the inbound message queue.
	var r reader
	var rOk bool

	// Latest message that has been decoded from the reader, but has not yet
	// been written to the inbound message queue.
	var m wire.Msg
	var mOk bool

	for {
		// If no reader is available, then block until an underlying network
		// connection is attached to the Channel, or until the context is done.
		// There is no point continuing until a reader is available, because
		// there is nothing from which to read new messages.
		if !rOk {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r, rOk = <-ch.readers:
				// Do nothing, which will cause us to move on to reading
				// messages.
			}
		}

		// If we do not already have a message that we are trying to write to
		// the inbound message channel, then attempt to read one.
		if !mOk {
			n, err := r.Decoder(r.Conn, buf[:])
			if err != nil {
				// If reading from the reader fails, then clear the reader. This
				// will cause the next iteration to wait until a new underlying
				// network connection is attached to the Channel.
				close(r.q)
				r = reader{}
				rOk = false
				continue
			}
			// Unmarshal the message from binary. If this is successfully, then
			// we mark the message as available (and will attempt to write it to
			// the inbound message channel).
			if _, _, err := m.Unmarshal(buf[:n], len(buf)); err != nil {
				continue
			}
			mOk = true
		}

		// At this point, a message is guaranteed to be available, so we attempt
		// to write it to the inbound message channel.
		select {
		case <-ctx.Done():
			if r.q != nil {
				close(r.q)
			}
			return ctx.Err()
		case ch.inbound <- m:
			// If we succeed, then we clear the message. This will allow us to
			// progress and try to read the next message.
			m = wire.Msg{}
			mOk = false
		case v, vOk := <-ch.readers:
			// If a new underlying network connection is attached to the
			// Channel before we can write the message to the inbound message
			// channel, we do not clear the message. This will force us to
			// re-attempt writing the message in the next iteration.
			if r.q != nil {
				close(r.q)
			}
			r, rOk = v, vOk
		}
	}
}

func (ch Channel) writeLoop(ctx context.Context) {
	var buf [4194304]byte

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
			// Marshal the latest message into bytes and write it to the
			_, n, err := m.Marshal(buf[:], len(buf))
			if err != nil {
				// Clear the latest message so that we can move on to other
				// messages. We do this, because failure to marshal is not
				// something that is typically recoverable.
				m = wire.Msg{}
				mOk = false
				continue
			}
			if _, err := w.Encoder(w.Conn, buf[:n]); err != nil {
				// If an error happened when trying to write to the writer,
				// then clean the writer. This will force the Channel to
				// block on future writes until a new network connection is
				// attached. The latest message is not replaced (so we will
				// re-attempt to write it when a new connection is
				// eventually attached).
				close(w.q)
				w = writer{}
				wOk = false
				continue
			}
			// Clear the latest message so that we can move on to other
			// messages.
			m = wire.Msg{}
			mOk = false
		}
	}
}
