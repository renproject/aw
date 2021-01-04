package channel

import (
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	DefaultDrainTimeout       = 30 * time.Second
	DefaultDrainInBackground  = true
	DefaultMaxMessageSize     = 4 * 1024 * 1024         // 4MB
	DefaultRateLimit          = rate.Limit(1024 * 1024) // 1MB per second
	DefaultInboundBufferSize  = 0
	DefaultOutboundBufferSize = 0
)

// Options for parameterizing the behaviour of a Channel.
type Options struct {
	Logger             *zap.Logger
	DrainTimeout       time.Duration
	DrainInBackground  bool
	MaxMessageSize     int
	RateLimit          rate.Limit
	InboundBufferSize  int
	OutboundBufferSize int
}

// DefaultOptions returns Options with sane defaults.
func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return Options{
		Logger:             logger,
		DrainTimeout:       DefaultDrainTimeout,
		DrainInBackground:  DefaultDrainInBackground,
		MaxMessageSize:     DefaultMaxMessageSize,
		RateLimit:          DefaultRateLimit,
		InboundBufferSize:  DefaultInboundBufferSize,
		OutboundBufferSize: DefaultOutboundBufferSize,
	}
}

// WithLogger sets the Logger used for logging all errors, warnings, information,
// debug traces, and so on.
func (opts Options) WithLogger(logger *zap.Logger) Options {
	opts.Logger = logger
	return opts
}

// WithDrainTimeout sets the timeout used by the Channel when draining replaced
// connections. If a Channel does not see a message on a draining connection
// before the timeout, then the draining connection is dropped and closed, and
// all future messages sent to the connection will be lost.
func (opts Options) WithDrainTimeout(timeout time.Duration) Options {
	opts.DrainTimeout = timeout
	return opts
}

// WithDrainInBackground enables/disable background draining of replaced
// connections. Setting this to true can improve performance, but it also break
// the deliver order of messages.
func (opts Options) WithDrainInBackground(enable bool) Options {
	opts.DrainInBackground = enable
	return opts
}

// WithMaxMessageSize sets the maximum number of bytes that a channel will read
// at one time. This number restricts the maximum message size that remote peers
// can send, defines the buffer size used for unmarshalling messages, and
// defines the rate limit burst.
func (opts Options) WithMaxMessageSize(maxMessageSize int) Options {
	opts.MaxMessageSize = maxMessageSize
	return opts
}

// WithRateLimit sets the bytes-per-second rate limit that will be enforced on
// all network connections. If a network connection exceeds this limit, then the
// connection will be closed, and a new one will need to be established.
func (opts Options) WithRateLimit(rateLimit rate.Limit) Options {
	opts.RateLimit = rateLimit
	return opts
}

// WithInboundBufferSize defines the number of inbound messages that can be
// buffered in memory before back-pressure will prevent the buffering of new
// inbound messages.
func (opts Options) WithInboundBufferSize(size int) Options {
	opts.InboundBufferSize = size
	return opts
}

// WithOutboundBufferSize defines the number of outbound messages that can be
// buffered in memroy before back-pressure will prevent the buffering of new
// outbound messages.
func (opts Options) WithOutboundBufferSize(size int) Options {
	opts.OutboundBufferSize = size
	return opts
}
