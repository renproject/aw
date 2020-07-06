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
	"github.com/renproject/id"
	"github.com/renproject/surge"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	DefaultServerTimeout    = 10 * time.Second
	DefaultServerTimeToLive = 24 * time.Hour
	DefaultServerMaxConns   = 128
	DefaultServerHost       = "0.0.0.0"
	DefaultServerPort       = uint16(18514)

	DefaultConnRateLimit      = rate.Limit(0.1)
	DefaultConnRateLimitBurst = 10

	DefaultBandwidthLimit      = rate.Limit(4 * 1024 * 1024) // 4 MB
	DefaultBandwidthLimitBurst = 32 * 1024 * 1024            // 32 MB
)

type ServerOptions struct {
	Logger              *zap.Logger
	Timeout             time.Duration
	TimeToLive          time.Duration
	MaxConns            int
	Host                string
	Port                uint16
	RateLimit           rate.Limit
	RateLimitBurst      int
	BandwidthLimit      rate.Limit
	BandwidthLimitBurst int
}

func DefaultServerOptions() ServerOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return ServerOptions{
		Logger:              logger,
		Timeout:             DefaultServerTimeout,
		TimeToLive:          DefaultServerTimeToLive,
		MaxConns:            DefaultServerMaxConns,
		Host:                DefaultServerHost,
		Port:                DefaultServerPort,
		RateLimit:           DefaultConnRateLimit,
		RateLimitBurst:      DefaultConnRateLimitBurst,
		BandwidthLimit:      DefaultBandwidthLimit,
		BandwidthLimitBurst: DefaultBandwidthLimitBurst,
	}
}

// WithLogger sets the logger that will be used by the server.
func (opts ServerOptions) WithLogger(logger *zap.Logger) ServerOptions {
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

	// pool of connections.
	pool ServerConnPool

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

		pool: NewServerConnPool(int64(opts.MaxConns)),

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
	server.opts.Logger.Info(
		"listening",
		zap.String("host", server.opts.Host),
		zap.Uint16("port", server.opts.Port))

	// Attempt to listen for incoming connections on the configured host and
	// port. Return any errors that occur.
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", server.opts.Host, server.opts.Port))
	if err != nil {
		return fmt.Errorf("listening on %v:%v: %v", server.opts.Host, server.opts.Port, err)
	}

	go func() {
		// When the context is done, explicitly close the listener so that it
		// does not block on waiting to accept a new connection.
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.opts.Logger.Error("closing listener", zap.Error(err))
		}
	}()

	for {
		// Wait until a connection slot becomes available before attempting to
		// accept a new connection.
		server.pool.Wait()

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Do not log errors because returning from this canceling a
				// context is the expected way to terminate the run loop.
				return nil
			default:
			}
			server.opts.Logger.Error("accepting connection", zap.Error(err))
			continue
		}

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections.
		go server.handle(ctx, conn)
	}
}

func (server *Server) handle(ctx context.Context, conn net.Conn) {
	// Close the connection and signal that the connection slot being consumed
	// is no longer needed.
	defer server.pool.Signal()
	defer conn.Close()

	// Reject connections from IP-addresses that have attempted to connect too
	// recently.
	var remoteAddr string
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteAddr = tcpAddr.IP.String()
	} else {
		remoteAddr = conn.RemoteAddr().String()
	}
	if !server.allowRateLimit(remoteAddr) {
		server.opts.Logger.Info("limiting", zap.String("remote", remoteAddr))
		return
	}
	remoteAddrToken := server.pool.AddToRemoteAddress(remoteAddr, conn)
	defer server.pool.RemoveFromRemoteAddress(remoteAddr, remoteAddrToken)

	// Handshake with the client to establish an authenticated and encrypted
	// session.
	sessionCtx, cancel := context.WithTimeout(ctx, server.opts.Timeout)
	defer cancel()
	session, err := server.handshaker.AcceptHandshake(sessionCtx, conn)
	if err != nil {
		server.opts.Logger.Error("accepting handshake", zap.Error(err))
		return
	}
	remoteSignatory := session.RemoteSignatory()
	remoteSignatoryToken := server.pool.AddToRemoteSignatory(remoteSignatory, conn)
	defer server.pool.RemoveFromRemoteSignatory(remoteSignatory, remoteSignatoryToken)

	// Read messages from the client until the time-to-live expires, or an error
	// is encountered when trying to read.
	server.opts.Logger.Info("handling", zap.String("remote", remoteAddr))
	bufReader := bufio.NewReaderSize(conn, surge.MaxBytes)
	bufWriter := bufio.NewWriterSize(conn, surge.MaxBytes)
	for {
		// We have "time-to-live" amount of time to read a message and write a
		// response to the message.
		if err := conn.SetDeadline(time.Now().Add(server.opts.TimeToLive)); err != nil {
			server.opts.Logger.Error("setting deadline", zap.Error(err))
			return
		}

		// Read message from connection.
		msg := wire.Message{}
		if _, err := msg.Unmarshal(bufReader, surge.MaxBytes); err != nil {
			if err != io.EOF {
				server.opts.Logger.Error("bad message", zap.Error(err))
				return
			}
			server.opts.Logger.Info("closing connection", zap.String("remote", conn.RemoteAddr().String()))
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
			server.opts.Logger.Error("bad message", zap.Uint8("version", uint8(msg.Version)))
			return
		}
		// Check that the message type is supported.
		switch msg.Type {
		case wire.Ping, wire.Push, wire.Pull:
			// Ok; do nothing.
		case wire.PingAck, wire.PushAck, wire.PullAck:
			// Not ok; only clients expect to receive acks.
			server.opts.Logger.Error("bad message ack", zap.Uint8("type", uint8(msg.Type)))
			return
		default:
			server.opts.Logger.Error("bad message", zap.Uint8("type", uint8(msg.Type)))
			return
		}

		// Decrypt message body. We do this after checking the version and the
		// type so that we do not waste precious CPU cycles on unsupported
		// messages.
		msg.Data, err = session.Decrypt(msg.Data)
		if err != nil {
			server.opts.Logger.Error("bad message", zap.Error(err))
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
			server.opts.Logger.Error("handling message", zap.Error(err))
			return
		}
		// Ok returned from the listeners indicates that we should send a
		// response to the client. We must encrypt the response before
		// sending it.
		response.Data, err = session.Encrypt(response.Data)
		if err != nil {
			server.opts.Logger.Error("bad response: %v", zap.Error(err))
			return
		}
		if _, err := response.Marshal(bufWriter, surge.MaxBytes); err != nil {
			server.opts.Logger.Error("bad response", zap.Error(err))
			return
		}
		if err := bufWriter.Flush(); err != nil {
			server.opts.Logger.Error("flushing", zap.Error(err))
			return
		}
	}
}

func (server *Server) allowRateLimit(remoteAddr string) bool {
	server.rateLimitsMu.Lock()
	defer server.rateLimitsMu.Unlock()

	if limiter, ok := server.rateLimitsFront[remoteAddr]; ok {
		return limiter.Allow()
	}
	if limiter, ok := server.rateLimitsBack[remoteAddr]; ok {
		return limiter.Allow()
	}

	if len(server.rateLimitsFront) >= server.rateLimitsCapacity {
		server.rateLimitsBack = server.rateLimitsFront
		server.rateLimitsFront = make(map[string]*rate.Limiter, server.rateLimitsCapacity)
	}
	server.rateLimitsFront[remoteAddr] = rate.NewLimiter(server.opts.RateLimit, server.opts.RateLimitBurst)
	return server.rateLimitsFront[remoteAddr].Allow()
}

// A ServerConnPool is used to manage connections from the perspective of a
// server. Its primary responsibility is ensuring that every IP address and
// every cryptographic identity only has one active connection, where new
// connections will replace old connections.
type ServerConnPool struct {
	numConnsCond *sync.Cond
	numConns     int64
	maxConns     int64

	perAddrMu   *sync.Mutex
	perAddr     map[string]net.Conn
	perAddrToks map[string]uint64

	perSignatoryMu   *sync.Mutex
	perSignatory     map[id.Signatory]net.Conn
	perSignatoryToks map[id.Signatory]uint64

	nextToken uint64
}

// NewServerConnPool returns an empty connection pool. Because this pool exists
// to prevent duplicate connections, and connections do not persist across
// reboots, the pool does not need to be persisted across reboots. In other
// words, this method is sufficient to construct a new connection pool.
func NewServerConnPool(maxConns int64) ServerConnPool {
	return ServerConnPool{
		numConnsCond: sync.NewCond(new(sync.Mutex)),
		numConns:     0,
		maxConns:     maxConns,

		perAddrMu:   new(sync.Mutex),
		perAddr:     map[string]net.Conn{},
		perAddrToks: map[string]uint64{},

		perSignatoryMu:   new(sync.Mutex),
		perSignatory:     map[id.Signatory]net.Conn{},
		perSignatoryToks: map[id.Signatory]uint64{},

		nextToken: 0,
	}
}

// Wait until the there are available connections. If the maximum number of
// connections have already been established, then this method will block until
// one of the existing connections is closed.
func (pool *ServerConnPool) Wait() {
	pool.numConnsCond.L.Lock()
	for atomic.LoadInt64(&pool.numConns) >= pool.maxConns {
		pool.numConnsCond.Wait()
	}
	pool.numConnsCond.L.Unlock()
	atomic.AddInt64(&pool.numConns, 1)
}

// Signal that an existing connection has been closed.
func (pool *ServerConnPool) Signal() {
	atomic.AddInt64(&pool.numConns, -1)
	pool.numConnsCond.Signal()
}

// AddToRemoteAddress will add a connection and associate it with an IP address.
// Any previous connection associated with this IP address will be closed (and
// error caused by the closure will be ignored). This method returns a token
// that can be used to explicitly remove the connection.
func (pool *ServerConnPool) AddToRemoteAddress(ip string, conn net.Conn) uint64 {
	pool.perAddrMu.Lock()
	defer pool.perAddrMu.Unlock()

	token := atomic.AddUint64(&pool.nextToken, 1)

	if prevConn, ok := pool.perAddr[ip]; ok {
		if prevConn != nil {
			prevConn.Close()
		}
		delete(pool.perAddr, ip)
		delete(pool.perAddrToks, ip)
	}
	if conn != nil {
		pool.perAddr[ip] = conn
		pool.perAddrToks[ip] = token
	}

	return token
}

// AddToRemoteSignatory will add a connection and associate it with a signatory.
// Any previous connection associated with this signatory will be closed (any
// error caused by the closure will be ignored). This method returns a token
// that can be used to explicitly remove the connection.
func (pool *ServerConnPool) AddToRemoteSignatory(signatory id.Signatory, conn net.Conn) uint64 {
	pool.perSignatoryMu.Lock()
	defer pool.perSignatoryMu.Unlock()

	token := atomic.AddUint64(&pool.nextToken, 1)

	if prevConn, ok := pool.perSignatory[signatory]; ok {
		if prevConn != nil {
			prevConn.Close()
		}
		delete(pool.perSignatory, signatory)
		delete(pool.perSignatoryToks, signatory)
	}
	if conn != nil {
		pool.perSignatory[signatory] = conn
		pool.perSignatoryToks[signatory] = token
	}

	return token
}

// RemoveFromRemoteAddress removes an IP address from the map, unless the
// provided token is no longer the most recent token, and closes any associated
// connection (any error caused by the closure will be ignored).
func (pool *ServerConnPool) RemoveFromRemoteAddress(ip string, tok uint64) {
	pool.perAddrMu.Lock()
	defer pool.perAddrMu.Unlock()

	if currTok, ok := pool.perAddrToks[ip]; ok && currTok == tok {
		if prevConn, ok := pool.perAddr[ip]; ok {
			if prevConn != nil {
				prevConn.Close()
			}
		}
		delete(pool.perAddr, ip)
		delete(pool.perAddrToks, ip)
	}
}

// RemoveFromRemoteSignatory removes a signatory from the map, unless the
// provided token is no longer the most recent token, and closes any associated
// connection (any error caused by the closure will be ignored).
func (pool *ServerConnPool) RemoveFromRemoteSignatory(signatory id.Signatory, tok uint64) {
	pool.perSignatoryMu.Lock()
	defer pool.perSignatoryMu.Unlock()

	if currTok, ok := pool.perSignatoryToks[signatory]; ok && currTok == tok {
		if prevConn, ok := pool.perSignatory[signatory]; ok {
			if prevConn != nil {
				prevConn.Close()
			}
		}
		delete(pool.perSignatory, signatory)
		delete(pool.perSignatoryToks, signatory)
	}
}
