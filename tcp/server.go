package tcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/wire"
	"github.com/renproject/surge"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	DefaultServerTimeout    = 10 * time.Second
	DefaultServerTimeToLive = 24 * time.Hour
	DefaultServerMaxConns   = 128
	DefaultServerHost       = "0.0.0.0"
	DefaultServerPort       = uint16(18514)
	DefaultRateLimit        = rate.Limit(1.0)
	DefaultRateLimitBurst   = DefaultServerMaxConns
)

type ServerOptions struct {
	Logger         logrus.FieldLogger
	Timeout        time.Duration
	TimeToLive     time.Duration
	MaxConns       int
	Host           string
	Port           uint16
	RateLimit      rate.Limit
	RateLimitBurst int
}

func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "tcp").
			WithField("com", "server"),
		Timeout:        DefaultServerTimeout,
		TimeToLive:     DefaultServerTimeToLive,
		MaxConns:       DefaultServerMaxConns,
		Host:           DefaultServerHost,
		Port:           DefaultServerPort,
		RateLimit:      DefaultRateLimit,
		RateLimitBurst: DefaultRateLimitBurst,
	}
}

// WithLogger sets the logger.
func (opts ServerOptions) WithLogger(logger logrus.FieldLogger) ServerOptions {
	opts.Logger = logger
	return opts
}

// WithHost sets the host address that will be used for listening.
func (opts ServerOptions) WithHost(host string) ServerOptions {
	opts.Host = host
	return opts
}

// WithPort sets the port that will be used for listening.
func (opts ServerOptions) WithPort(port uint16) ServerOptions {
	opts.Port = port
	return opts
}

type Server struct {
	opts ServerOptions

	handshaker handshake.Handshaker
	listener   wire.Listener

	// numConns must only be accessed atomically.
	numConns int64

	// rateLimits must only be accessed while the mutex is locked.
	rateLimitsMu       *sync.Mutex
	rateLimitsFront    map[string]*rate.Limiter // Front is used to add new limiters until the max capacity is reached.
	rateLimitsBack     map[string]*rate.Limiter // Back is used to read old limiters that have been rotated from the front.
	rateLimitsCapacity int
}

func NewServer(opts ServerOptions, handshaker handshake.Handshaker, listener wire.Listener) *Server {
	return &Server{
		opts: opts,

		handshaker: handshaker,
		listener:   listener,

		numConns: 0,

		rateLimitsMu:       new(sync.Mutex),
		rateLimitsFront:    make(map[string]*rate.Limiter, 65535),
		rateLimitsBack:     make(map[string]*rate.Limiter, 0),
		rateLimitsCapacity: 65535,
	}
}

// Options returns the Options used to configure the Server. Changing the
// Options returned by the method will have no affect on the behaviour of the
// Server.
func (server *Server) Options() ServerOptions {
	return server.opts
}

// Listen for incoming connections until the context is done. The Server will
// accept spawn a background goroutine for every accepted connection, but will
// not accept more connections than its configured maximum.
func (server *Server) Listen(ctx context.Context) error {
	// Attempt to listen for incoming connections on the configured host and
	// port. Return any errors that occur.
	server.opts.Logger.Infof("listening on %v:%v", server.opts.Host, server.opts.Port)
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", server.opts.Host, server.opts.Port))
	if err != nil {
		return fmt.Errorf("error listening on %v:%v: %v", server.opts.Host, server.opts.Port, err)
	}

	go func() {
		// When the context is done, explicitly close the listener so that it
		// does not block on waiting to accept a new connection.
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.opts.Logger.Errorf("error closing listener: %v", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Do not log errors because returning from this canceling a
				// context is the expected way to terminate the run loop.
				return nil
			default:
			}

			server.opts.Logger.Errorf("accepting connection: %v", err)
			continue
		}
		if atomic.LoadInt64(&server.numConns) >= int64(server.opts.MaxConns) {
			server.opts.Logger.Infof("closing connection: max inbound connections exceeded")
			if err := conn.Close(); err != nil {
				server.opts.Logger.Errorf("closing connection: %v", err)
			}
			continue
		}
		atomic.AddInt64(&server.numConns, 1)

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections.
		go server.handle(ctx, conn)
	}
}

func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer atomic.AddInt64(&server.numConns, -1)
	defer conn.Close()

	// Reject connections from IP-addresses that have attempted to connect too
	// recently.
	if !server.allowRateLimit(conn) {
		server.opts.Logger.Infof("handling connection: remote=%v has been rate-limited", conn.RemoteAddr().String())
		return
	}
	server.opts.Logger.Infof("handling connection: remote=%v", conn.RemoteAddr().String())

	innerCtx, cancel := context.WithTimeout(ctx, server.opts.Timeout)
	defer cancel()

	// Handshake with the client to establish an authenticated and encrypted
	// session.
	session, err := server.handshaker.AcceptHandshake(innerCtx, conn)
	if err != nil {
		server.opts.Logger.Errorf("accepting handshake: %v", err)
		return
	}
	if session == nil {
		server.opts.Logger.Errorf("accepting handshake: nil")
		return
	}
	// TODO: Restrict the number of inbound connection from the remote
	// signatory. Alternatively, drop all previous connections from the remote
	// signatory, implicitly restricting them to only one (the latest)
	// connection.
	//
	//	session.RemoteSignatory()
	//

	// Read messages from the client until the time-to-live expires, or an error
	// is encountered when trying to read.
	bufReader := bufio.NewReaderSize(conn, surge.MaxBytes)
	bufWriter := bufio.NewWriterSize(conn, surge.MaxBytes)
	for {
		// We have "time-to-live" amount of time to read a message and write a
		// response to the message.
		if err := conn.SetDeadline(time.Now().Add(server.opts.TimeToLive)); err != nil {
			server.opts.Logger.Errorf("setting deadline: %v", err)
			return
		}

		// Read message from connection.
		msg := wire.Message{}
		if _, err := msg.Unmarshal(bufReader, surge.MaxBytes); err != nil {
			if err != io.EOF {
				server.opts.Logger.Errorf("unmarshaling message: %v", err)
				return
			}
			server.opts.Logger.Infof("closing connectin: %v", err)
			return
		}

		// FIXME: Check rate-limiting from this connection to protect against
		// spam. If the rate-limit is hit too many times, then the connection
		// must be dropped, and the IP-address should be black-listed for a
		// period of time.

		// Check that the message version is supported.
		switch msg.Version {
		case wire.V1:
			// Ok; do nothing.
		default:
			server.opts.Logger.Errorf("checking message: unsupported version=%v", msg.Version)
			return
		}
		// Check that the message type is supported.
		switch msg.Type {
		case wire.Ping, wire.Push, wire.Pull:
			// Ok; do nothing.
		case wire.PingAck, wire.PushAck, wire.PullAck:
			// Not ok; only clients expect to receive acks.
			server.opts.Logger.Errorf("checking message: server does not expect ack=%v", msg.Type)
			return
		default:
			server.opts.Logger.Errorf("checking message: unsupported type=%v", msg.Type)
			return
		}

		// Decrypt message body. We do this after checking the version and the
		// type so that we do not waste precious CPU cycles on unsupported
		// messages.
		msg.Data, err = session.Decrypt(msg.Data)
		if err != nil {
			server.opts.Logger.Errorf("decrypting message: %v", err)
			return
		}

		var response wire.Message
		var err error
		switch msg.Type {
		case wire.Ping:
			response, err = server.listener.DidReceivePing(msg.Version, msg.Data, session.RemoteSignatory())
		case wire.Push:
			response, err = server.listener.DidReceivePush(msg.Version, msg.Data, session.RemoteSignatory())
		case wire.Pull:
			response, err = server.listener.DidReceivePull(msg.Version, msg.Data, session.RemoteSignatory())
		default:
			panic("unreachable")
		}

		if err != nil {
			// An error returned from the listeners indicates that the
			// connection should be killed immediately.
			server.opts.Logger.Errorf("handling message: %v", err)
			return
		}
		// Ok returned from the listeners indicates that we should send a
		// response to the client. We must encrypt the response before
		// sending it.
		response.Data, err = session.Encrypt(response.Data)
		if err != nil {
			server.opts.Logger.Errorf("encrypting response: %v", err)
			return
		}
		if _, err := response.Marshal(bufWriter, surge.MaxBytes); err != nil {
			server.opts.Logger.Errorf("marshaling response: %v", err)
			return
		}
		if err := bufWriter.Flush(); err != nil {
			server.opts.Logger.Errorf("flushing response: %v", err)
			return
		}
	}
}

func (server *Server) allowRateLimit(conn net.Conn) bool {
	var addr string
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		addr = tcpAddr.IP.String()
	} else {
		addr = conn.RemoteAddr().String()
	}

	server.rateLimitsMu.Lock()
	defer server.rateLimitsMu.Unlock()

	if limiter, ok := server.rateLimitsFront[addr]; ok {
		return limiter.Allow()
	}
	if limiter, ok := server.rateLimitsBack[addr]; ok {
		return limiter.Allow()
	}

	if len(server.rateLimitsFront) >= server.rateLimitsCapacity {
		server.rateLimitsBack = server.rateLimitsFront
		server.rateLimitsFront = make(map[string]*rate.Limiter, server.rateLimitsCapacity)
	}
	server.rateLimitsFront[addr] = rate.NewLimiter(server.opts.RateLimit, server.opts.RateLimitBurst)
	return server.rateLimitsFront[addr].Allow()
}
