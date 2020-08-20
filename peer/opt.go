package peer

import (
	"runtime"
	"time"

	"github.com/renproject/aw/wire"
	"go.uber.org/zap"
)

var (
	DefaultAddr                 = wire.NewUnsignedAddress(wire.TCP, "0.0.0.0:18514", uint64(time.Now().Unix()))
	DefaultAlpha                = 10
	DefaultPingTimeout          = 10 * time.Second
	DefaultPingInterval         = 10 * time.Minute
	DefaultNumBackgroundWorkers = 10 * runtime.NumCPU()
)

type Options struct {
	Logger *zap.Logger

	Addr                 wire.Address
	Alpha                int
	PingTimeout          time.Duration
	PingInterval         time.Duration
	NumBackgroundWorkers int
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return Options{
		Logger: logger,

		Addr:                 DefaultAddr,
		Alpha:                DefaultAlpha,
		PingTimeout:          DefaultPingTimeout,
		PingInterval:         DefaultPingInterval,
		NumBackgroundWorkers: DefaultNumBackgroundWorkers,
	}
}

func (opts Options) WithLogger(logger *zap.Logger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithAddr(addr wire.Address) Options {
	opts.Addr = addr
	return opts
}

func (opts Options) WithAlpha(alpha int) Options {
	opts.Alpha = alpha
	return opts
}

func (opts Options) WithPingTimeout(pingTimeout time.Duration) Options {
	opts.PingTimeout = pingTimeout
	return opts
}

func (opts Options) WithPingInterval(pingInterval time.Duration) Options {
	opts.PingInterval = pingInterval
	return opts
}

func (opts Options) WithNumBackgroundWorkers(numBackgroundWorkers int) Options {
	opts.NumBackgroundWorkers = numBackgroundWorkers
	return opts
}
