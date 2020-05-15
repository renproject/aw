package gossip

import (
	"time"

	"github.com/sirupsen/logrus"
)

var (
	DefaultAlpha   = 10
	DefaultBias    = 0.25
	DefaultTimeout = 5 * time.Second
)

type Options struct {
	Logger logrus.FieldLogger

	Alpha   int
	Bias    float64
	Timeout time.Duration
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "gossip").
			WithField("com", "gossiper"),
		Alpha:   DefaultAlpha,
		Bias:    DefaultBias,
		Timeout: DefaultTimeout,
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
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
