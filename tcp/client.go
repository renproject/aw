package tcp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/wire"
	"github.com/renproject/surge"
	"github.com/sirupsen/logrus"
)

var (
	// DefaultClientTimeToLive is set to 1 hour. If no dial attempt succeeds, or
	// no messages are attempted to be sent, within one hour, then the
	// connection is killed and all pending messages will be lost.
	DefaultClientTimeToLive = time.Hour
	// DefaultClientTimeToDial is set to 30 seconds. If a single dial attempt
	// takes longer than this duration, it will be dropped (and a new attempt
	// may begin).
	DefaultClientTimeToDial = 30 * time.Second
	// DefaultClientMaxCapacity is set to 4096.
	DefaultClientMaxCapacity = 4096
)

// ClientOptions are used to parameterize the behaviour of the Client.
type ClientOptions struct {
	// Logger for all information/debugging/error output.
	Logger logrus.FieldLogger
	// TimeToLive for connections. Connections with no activity after the
	// TimeToLive duration will be killed. Setting the TimeToLive, and carefully
	// choosing which addresses to send messages to, is the primary mechanism
	// for ensuring that the Client does not consume too many resources on
	// maintaining connections (there is no explicit maximum number of
	// connections). This is also the amount of time that we will wait while
	// attempting to dial connections.
	TimeToLive time.Duration
	// TimeToDial for establishing new connections. After this duration has
	// passed, the dial attempt will be dropped. A new dial attempt will usually
	// be started, assuming that the client has not been attempting dials for
	// longer than the TimeToLive.
	TimeToDial time.Duration
	// MaxCapacity of messages that can be bufferred while waiting to write
	// messages to a channel.
	MaxCapacity int
}

// DefaultClientOptions return the default ClientOptions.
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "tcp").
			WithField("com", "client"),
		TimeToLive:  DefaultClientTimeToLive,
		TimeToDial:  DefaultClientTimeToDial,
		MaxCapacity: DefaultClientMaxCapacity,
	}
}

func (opts ClientOptions) WithLogger(logger logrus.FieldLogger) ClientOptions {
	opts.Logger = logger
	return opts
}

func (opts ClientOptions) WithTimeToLive(ttl time.Duration) ClientOptions {
	opts.TimeToLive = ttl
	return opts
}

func (opts ClientOptions) WithMaxCapacity(capacity int) ClientOptions {
	opts.MaxCapacity = capacity
	return opts
}

type conn struct {
	ch     chan<- wire.Message
	cancel context.CancelFunc
}

type Client struct {
	opts ClientOptions

	handshaker handshake.Handshaker
	listener   wire.Listener

	connsByAddrMu *sync.Mutex
	connsByAddr   map[string]conn
}

func NewClient(opts ClientOptions, handshaker handshake.Handshaker, listener wire.Listener) *Client {
	client := &Client{
		opts: opts,

		handshaker: handshaker,
		listener:   listener,

		connsByAddrMu: new(sync.Mutex),
		connsByAddr:   map[string]conn{},
	}
	return client
}

// Options returns the Options used to configure the Client. Changing the
// Options returned by the method will have no affect on the behaviour of the
// Client.
func (client *Client) Options() ClientOptions {
	return client.opts
}

// Send a message to an address. If no connection exists between the Client and
// the address, then one will be established. If a connection already exists, it
// will be re-used. If the message cannot be sent before the context is done,
// then an error will be returned.
func (client *Client) Send(ctx context.Context, addr string, msg wire.Message) error {
	client.connsByAddrMu.Lock()
	defer client.connsByAddrMu.Unlock()

	// Get an existing connection, or establish and maintain a new connection.
	// Running (and keeping alive) a new connection is non-blocking, and will
	// immediately return a connection object. Messages sent to the connection
	// while it is still being established will be buffered. If the MaxCapacity
	// number of messages has been buffered, new messages will be dropped (this
	// can be configured in the ClientOptions).
	conn, ok := client.connsByAddr[addr]
	if !ok {
		client.connsByAddr[addr] = client.runAndKeepAlive(addr)
		conn = client.connsByAddr[addr]
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case conn.ch <- msg:
		return nil
	}
}

// Close all connections being maintained by the Client to this address. Other
// connections will be kept alive.
func (client *Client) Close(addr string) {
	client.connsByAddrMu.Lock()
	defer client.connsByAddrMu.Unlock()

	// Cancel the connection and remove it from memory.
	if conn, ok := client.connsByAddr[addr]; ok {
		conn.cancel()
		delete(client.connsByAddr, addr)
	}
}

// CloseAll connections being maintained by the Client.
func (client *Client) CloseAll() {
	client.connsByAddrMu.Lock()
	defer client.connsByAddrMu.Unlock()

	// Cancel all connections and then remove all of them from memory. This is
	// more efficient than removing them one-by-one, and only requires one mutex
	// lock.
	for _, conn := range client.connsByAddr {
		conn.cancel()
	}
	client.connsByAddr = map[string]conn{}
}

// runAndKeepAlive will establish a new connection to the address, and keep the
// connection alive (including re-establishing dropped connections) until (a)
// the connection is explicitly cancelled by calling Close or CloseAll, (b) the
// connection cannot be established (or re-established) after the TimeToLive
// duration, or (c) no message has been sent to the connection for the
// TimeToLive duration.
func (client *Client) runAndKeepAlive(addr string) conn {
	ch := make(chan wire.Message, client.opts.MaxCapacity)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer client.Close(addr)

		// Remember the last message that was sent on this connection. To
		// guarantee at-least-once delivery, we will resend this message when we
		// re-run the connection.
		var lastMessageSent *wire.Message
		var err error

		// Loop until the context is cancelled, or until there is an
		// unrecoverable error. The only expected errors returned are those
		// caused by failures to connect/write for the TimeToLive duration.
		for {
			select {
			case <-ctx.Done():
				return
			default:
				lastMessageSent, err = client.run(ctx, addr, ch, lastMessageSent)
				if err != nil {
					select {
					case <-ctx.Done():
						// If the context is done, then we expect errors, so we
						// do not handle them.
					default:
						client.opts.Logger.Errorf("running connection: %v", err)
					}
					return
				}
			}
		}
	}()

	return conn{ch: ch, cancel: cancel}
}

// Returning an error here will kill the connection, the channel, and all
// pending messages will be lost.
func (client *Client) run(ctx context.Context, addr string, ch <-chan wire.Message, lastMessage *wire.Message) (*wire.Message, error) {
	conn, session, err := client.dial(ctx, addr)
	if err != nil {
		// If connecting returns an error, then running must return an error.
		// Connecting should only return an error if it cannot successful dial
		// within the time-to-live (or an explicit cancel happens).
		return nil, fmt.Errorf("connecting: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			client.opts.Logger.Errorf("closing connection: %v", err)
		}
	}()

	// Run the message loop until the context is cancelled, or the deadline is
	// exceeded.
	rw := bufio.NewReadWriter(
		bufio.NewReaderSize(conn, surge.MaxBytes),
		bufio.NewWriterSize(conn, surge.MaxBytes),
	)

	// deadline is used to kill this connection if no messages have been written
	// to it for the time-to-live duration.
	deadline := time.NewTimer(client.opts.TimeToLive)

	if lastMessage != nil {
		// Failing to write a message should not result in the
		// connection/channel being killed, and should not result in all pending
		// messages being lost. Therefore, we consume the error, and return a
		// nil-error.
		client.opts.Logger.Infof("resending last message sent")
		if err := client.write(session, rw, *lastMessage); err != nil {
			client.opts.Logger.Errorf("writing: %v", err)
			return nil, nil
		}
	}
	for {
		select {
		case <-ctx.Done():
			// The connection was explicitly cancelled.
			return nil, fmt.Errorf("killing connection: %v", ctx.Err())
		case <-deadline.C:
			// The connection was implicitly cancelled, because it there have
			// been no messages sent to this connection by the Client for the
			// time-to-live duration.
			return nil, fmt.Errorf("killing connection: time-to-live expired")
		case msg, ok := <-ch:
			if !ok {
				panic("channel is closed")
			}

			// Reset the deadline timer (as described by the
			// documentation for Reset, we call Stop before calling Reset).
			// We reset deadline because we have just received a
			// message for writing. This means that this connection is still
			// being used.
			if !deadline.Stop() {
				<-deadline.C
			}
			deadline.Reset(client.opts.TimeToLive)

			// Set the deadline by which we must successfully write a message
			// and receive a response on the connection. If we cannot write by
			// this deadline, the connection will be re-established.
			if err := conn.SetDeadline(time.Now().Add(client.opts.TimeToLive)); err != nil {
				client.opts.Logger.Errorf("setting deadline: %v", err)
				return &msg, nil
			}

			// Failing to write a message should not result in the
			// connection/channel being killed, and should not result in all pending
			// messages being lost. Therefore, we consume the error, and return a
			// nil-error.
			if err := client.write(session, rw, msg); err != nil {
				client.opts.Logger.Errorf("writing: %v", err)
				return &msg, nil
			}
		}
	}
}

func (client *Client) write(session handshake.Session, rw *bufio.ReadWriter, msg wire.Message) error {
	var err error

	//
	// Write message.
	//

	// Encrypt the message.
	msg.Data, err = session.Encrypt(msg.Data)
	if err != nil {
		return fmt.Errorf("encrypting message: %v", err)
	}
	// Write the encrypted message to the connection.
	if _, err := msg.Marshal(rw, surge.MaxBytes); err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	if err := rw.Flush(); err != nil {
		return fmt.Errorf("flushing message: %v", err)
	}

	//
	// Wait for acknowledgement.
	//

	// Read an encrypted response from the connection.
	response := wire.Message{}
	if _, err := response.Unmarshal(rw, surge.MaxBytes); err != nil {
		return fmt.Errorf("unmarshaling response: %v", err)
	}
	// Check that the response version is supported.
	switch response.Version {
	case wire.V1:
		// Ok; do nothing.
	default:
		return fmt.Errorf("checking response: unsupported version=%v", response.Version)
	}
	// Check that the response type is supported.
	switch response.Type {
	case wire.PingAck, wire.PushAck, wire.PullAck:
		// Ok; do nothing.
	case wire.Ping, wire.Push, wire.Pull:
		// Not ok; only servers expect to receive non-acks.
		return fmt.Errorf("checking response: client does not expect type=%v", response.Type)
	default:
		return fmt.Errorf("checking response: unsupported type=%v", response.Type)
	}
	// Decrypt response body. We do this after checking the version
	// and the type so that we do not waste precious CPU cycles on
	// unsupported responses.
	response.Data, err = session.Decrypt(response.Data)
	if err != nil {
		return fmt.Errorf("decrypting response: %v", err)
	}

	//
	// Handle acknowledgement.
	//

	switch response.Type {
	case wire.PingAck:
		err = client.listener.DidReceivePingAck(response.Version, response.Data, session.RemoteSignatory())
	case wire.PushAck:
		err = client.listener.DidReceivePushAck(response.Version, response.Data, session.RemoteSignatory())
	case wire.PullAck:
		err = client.listener.DidReceivePullAck(response.Version, response.Data, session.RemoteSignatory())
	default:
		panic("unreachable")
	}
	if err != nil {
		// An error returned from the listeners indicates that the
		// connection should be killed immediately.
		return fmt.Errorf("handling response: %v", err)
	}
	return nil
}

// dial a new connection to an address. This method will return an error if no
// dial attempt succeeeds after the TimeToLive duration.
func (client *Client) dial(ctx context.Context, addr string) (net.Conn, handshake.Session, error) {
	// Create context for all dial attempts. We will attempt to dial once every
	// 30 seconds, until we are successful, or until this context is done
	// (either because the input context was cancelled, or the TimeToLive
	// duration has passed).
	dialCtx, dialCancel := context.WithTimeout(ctx, client.opts.TimeToLive)
	defer dialCancel()

	for {
		select {
		case <-dialCtx.Done():
			// dial must only return an error if it cannot successful
			// dial within the TimeToLive duration. Othewise, it will keep
			// retrying.
			return nil, nil, dialCtx.Err()
		default:
		}

		// Attempt to dial a new connection for 30 seconds. If we are not
		// successful, then wait until the end of the 30 second timeout and try
		// again.
		innerDialCtx, innerDialCancel := context.WithTimeout(dialCtx, client.opts.TimeToDial)
		conn, err := new(net.Dialer).DialContext(innerDialCtx, "tcp", addr)
		if err != nil {
			// Make sure to wait until the entire 30 seconds has passed,
			// otherwise we might attempt to re-dial too quickly.
			<-innerDialCtx.Done()
			innerDialCancel()

			client.opts.Logger.Warnf("dialing: %v", err)
			continue
		}
		innerDialCancel()

		// Handshake with the server to establish authentication and encryption
		// on the connection.
		session, err := client.handshaker.Handshake(dialCtx, conn)
		if err != nil {
			client.opts.Logger.Errorf("handshaking: %v", err)
			if err := conn.Close(); err != nil {
				client.opts.Logger.Errorf("closing connection: %v", err)
			}
			continue
		}
		if session == nil {
			client.opts.Logger.Errorf("handshaking: nil")
			if err := conn.Close(); err != nil {
				client.opts.Logger.Errorf("closing connection: %v", err)
			}
			continue
		}

		return conn, session, nil
	}
}
