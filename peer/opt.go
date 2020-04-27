package peer

import (
	"runtime"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/sirupsen/logrus"
)

var (
	DefaultAddr                 = wire.NewUnsignedAddress(wire.TCP, "0.0.0.0", uint64(time.Now().Unix()))
	DefaultAlpha                = 10
	DefaultPingTimeout          = 10 * time.Second
	DefaultPingInterval         = 10 * time.Minute
	DefaultNumBackgroundWorkers = 10 * runtime.NumCPU()
)

type Options struct {
	Logger logrus.FieldLogger

	Addr                 wire.Address
	Alpha                int
	PingTimeout          time.Duration
	PingInterval         time.Duration
	NumBackgroundWorkers int
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "peer").
			WithField("com", "peer"),

		Addr:         DefaultAddr,
		Alpha:        DefaultAlpha,
		PingTimeout:  DefaultPingTimeout,
		PingInterval: DefaultPingInterval,
	}
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
