package tcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/message"
	"github.com/renproject/bound"
	"github.com/renproject/surge"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	DefaultServerHandshaker = handshake.NewInsecure()
	DefaultServerTimeout    = 10 * time.Second
	DefaultServerTimeToLive = 24 * time.Hour
	DefaultServerMaxConns   = 256
	DefaultServerHost       = "0.0.0.0"
	DefaultServerPort       = uint16(18514)
	DefaultRateLimit        = rate.Limit(1.0)
	DefaultRateLimitBurst   = DefaultServerMaxConns
)

type ServerOptions struct {
	Logger         logrus.FieldLogger
	Handshaker     handshake.Handshaker
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
		Logger:         logrus.New(),
		Handshaker:     DefaultServerHandshaker,
		Timeout:        DefaultServerTimeout,
		TimeToLive:     DefaultServerTimeToLive,
		MaxConns:       DefaultServerMaxConns,
		Host:           DefaultServerHost,
		Port:           DefaultServerPort,
		RateLimit:      DefaultRateLimit,
		RateLimitBurst: DefaultRateLimitBurst,
	}
}

func (opts ServerOptions) WithLogger(logger logrus.FieldLogger) ServerOptions {
	opts.Logger = logger
	return opts
}

func (opts ServerOptions) WithHandshaker(hs handshake.Handshaker) ServerOptions {
	opts.Handshaker = hs
	return opts
}

func (opts ServerOptions) WithHost(host string) ServerOptions {
	opts.Host = host
	return opts
}

func (opts ServerOptions) WithPort(port uint16) ServerOptions {
	opts.Port = port
	return opts
}

type Server struct {
	opts ServerOptions

	// Rate limiters for all IP address globally, and each IP address
	// individually.
	limitGlobal *rate.Limiter
	limits      *bound.Map

	// Number of connections must only be modified using atomic reads/writes.
	numConns int64
	// Output for all messages received from all clients.
	output chan<- message.Message
}

func NewServer(opts ServerOptions, output chan<- message.Message) *Server {
	return &Server{
		opts: opts,

		limitGlobal: rate.NewLimiter(rate.Limit(opts.MaxConns)*opts.RateLimit, opts.RateLimitBurst),
		limits:      bound.NewMap(opts.MaxConns),

		numConns: 0,
		output:   output,
	}
}

func (server *Server) Options() ServerOptions {
	return server.opts
}

// Listen for client connection until the context is done. The server will
// accept new connections and spawning each one into a background goroutine.
func (server *Server) Listen(ctx context.Context) {
	server.opts.Logger.Infof("listening on %v:%v...", server.opts.Host, server.opts.Port)
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", server.opts.Host, server.opts.Port))
	if err != nil {
		server.opts.Logger.Fatalf("listen on %v: %v", server.opts.Host, err)
		return
	}

	go func() {
		// When the context is done, explicitly close the listener so that it
		// does not block on waiting to accept a new connection.
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.opts.Logger.Errorf("closing listener: %v", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Do not log errors because returning from this canceling a
				// context is the expected way to terminate the run loop.
				return
			default:
			}

			server.opts.Logger.Errorf("error accepting connection: %v", err)
			continue
		}
		if atomic.LoadInt64(&server.numConns) >= int64(server.opts.MaxConns) {
			server.opts.Logger.Warn("max connections exceeded")
			conn.Close()
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

	// Reject connections from IP addresses that have attempted to connect too
	// recently.
	if !server.allowRateLimit(conn) {
		return
	}

	innerCtx, cancel := context.WithTimeout(ctx, server.opts.Timeout)
	defer cancel()

	// Handshake with the client to establish an authenticated and encrypted
	// session.
	session, err := server.opts.Handshaker.AcceptHandshake(innerCtx, conn)
	if err != nil {
		server.opts.Logger.Errorf("bad handshake: %v", err)
		return
	}
	if session == nil {
		server.opts.Logger.Errorf("bad session: nil")
		return
	}

	innerCtx, cancel = context.WithTimeout(ctx, server.opts.TimeToLive)
	defer cancel()

	// Read messages from the client until the time-to-live expires, or an error
	// is encountered when trying to read.
	buf := bufio.NewReaderSize(conn, 1024*1024) // Pre-allocate 1MB
	for {
		// Read message from connection.
		m := message.Message{}
		if err := surge.Unmarshal(&m, buf); err != nil {
			if err != io.EOF {
				server.opts.Logger.Infof("read message: %v", err)
				return
			}
			server.opts.Logger.Errorf("read message: %v", err)
			return
		}

		// Decrypt message body.
		m.Data, err = session.Decrypt(m.Data)
		if err != nil {
			server.opts.Logger.Errorf("decrypt: %v", err)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case server.output <- m:
		}
	}
}

func (server *Server) allowRateLimit(conn net.Conn) bool {
	// Get the remote address.
	var address string
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		address = addr.IP.String()
	} else {
		address = conn.RemoteAddr().String()
	}

	// Check the global rate limiter. This is important because we can only
	// store a bounded number of per-IP rate limiters.
	if !server.limitGlobal.Allow() {
		return false
	}

	// Get the rate limiter for this IP address and set it in the map so that
	// the LRU timestamps are reset for this IP address.
	limiter, ok := server.limits.Get(address)
	if !ok {
		limiter = rate.NewLimiter(server.opts.RateLimit, server.opts.RateLimitBurst)
	}
	server.limits.Set(address, limiter)

	// Check the per-IP rate limiter.
	return limiter.(*rate.Limiter).Allow()
}
