package gossip

import (
	"time"

	"github.com/sirupsen/logrus"
)

var (
	DefaultAlpha                = 10
	DefaultBias                 = 0.25
	DefaultTimeout              = 5 * time.Second
	DefaultMaxRandomSignatories = 10
)

type Options struct {
	Logger logrus.FieldLogger

	Alpha                int
	Bias                 float64
	Timeout              time.Duration
	MaxRandomSignatories int
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "gossip").
			WithField("com", "gossiper"),
		Alpha:                DefaultAlpha,
		Bias:                 DefaultBias,
		Timeout:              DefaultTimeout,
		MaxRandomSignatories: DefaultMaxRandomSignatories,
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

// WithMaxRandomSignatories returns new Options with the given maximum random
// signatories. This is the maximum number of random signatories that will be
// returned when interacting with a subnet using the DefaultHash.
func (opts Options) WithMaxRandomSignatories(maxRandomSignatories int) Options {
	opts.MaxRandomSignatories = maxRandomSignatories
	return opts
}
