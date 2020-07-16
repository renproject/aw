package handshake

import (
	"time"

	"github.com/renproject/id"
	"go.uber.org/zap"
)

var (
	DefaultTimeout = 30 * time.Second
	DefaultFilter  = Filter(nil)
)

type Options struct {
	Logger  *zap.Logger
	PrivKey *id.PrivKey
	Timeout time.Duration
	Filter  Filter
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return Options{
		Logger:  logger,
		PrivKey: id.NewPrivKey(),
		Timeout: DefaultTimeout,
		Filter:  DefaultFilter,
	}
}

func (opts Options) WithLogger(logger *zap.Logger) Options {
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
