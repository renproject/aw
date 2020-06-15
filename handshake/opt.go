package handshake

import (
	"time"

	"github.com/renproject/id"
	"github.com/sirupsen/logrus"
)

var (
	DefaultTimeout = 30 * time.Second
	DefaultFilter  = Filter(nil)
)

type Options struct {
	Logger  logrus.FieldLogger
	PrivKey *id.PrivKey
	Timeout time.Duration
	Filter  Filter
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "handshake").
			WithField("com", "handshaker"),
		PrivKey: id.NewPrivKey(),
		Timeout: DefaultTimeout,
		Filter:  DefaultFilter,
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithPrivKey(privKey *id.PrivKey) Options {
	opts.PrivKey = privKey
	return opts
}

func (opts Options) WithTimeout(timeout time.Duration) Options {
	opts.Timeout = timeout
	return opts
}

func (opts Options) WithFilter(filter Filter) Options {
	opts.Filter = filter
	return opts
}
