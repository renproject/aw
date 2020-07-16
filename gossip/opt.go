package gossip

import (
	"time"

	"go.uber.org/zap"
)

var (
	DefaultAlpha       = 10
	DefaultBias        = 0.25
	DefaultTimeout     = 5 * time.Second
	DefaultMaxCapacity = 4096
)

type Options struct {
	Logger *zap.Logger

	Alpha       int
	Bias        float64
	Timeout     time.Duration
	MaxCapacity int
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return Options{
		Logger:      logger,
		Alpha:       DefaultAlpha,
		Bias:        DefaultBias,
		Timeout:     DefaultTimeout,
		MaxCapacity: DefaultMaxCapacity,
	}
}

func (opts Options) WithLogger(logger *zap.Logger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithAlpha(alpha int) Options {
	opts.Alpha = alpha
	return opts
}

func (opts Options) WithBias(bias float64) Options {
	opts.Bias = bias
	return opts
}

func (opts Options) WithTimeout(timeout time.Duration) Options {
	opts.Timeout = timeout
	return opts
}

func (opts Options) WithMaxCapacity(capacity int) Options {
	opts.MaxCapacity = capacity
	return opts
}
