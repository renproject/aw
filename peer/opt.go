package peer

import (
	"time"

	"github.com/renproject/id"

	"go.uber.org/zap"
)

type SyncerOptions struct {
	Logger  *zap.Logger
	Alpha   int
	Timeout time.Duration
}

func DefaultSyncerOptions() SyncerOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return SyncerOptions{
		Logger:  logger,
		Alpha:   DefaultAlpha,
		Timeout: DefaultTimeout,
	}
}

func (opts SyncerOptions) WithLogger(logger *zap.Logger) SyncerOptions {
	opts.Logger = logger
	return opts
}

func (opts SyncerOptions) WithAlpha(alpha int) SyncerOptions {
	opts.Alpha = alpha
	return opts
}

func (opts SyncerOptions) WithTimeout(timeout time.Duration) SyncerOptions {
	opts.Timeout = timeout
	return opts
}

type GossiperOptions struct {
	Logger  *zap.Logger
	Alpha   int
	Timeout time.Duration
}

func DefaultGossiperOptions() GossiperOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return GossiperOptions{
		Logger:  logger,
		Alpha:   DefaultAlpha,
		Timeout: DefaultTimeout,
	}
}

type DiscoveryOptions struct {
	Logger  *zap.Logger
	Alpha   int
	MaxExpectedPeers int
	PingTimePeriod time.Duration
}

func DefaultDiscoveryOptions() DiscoveryOptions {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return DiscoveryOptions{
		Logger:  logger,
		Alpha:   DefaultAlpha,
		MaxExpectedPeers: DefaultAlpha,
		PingTimePeriod: DefaultTimeout,
	}
}

type Options struct {
	SyncerOptions
	GossiperOptions
	DiscoveryOptions

	Logger  *zap.Logger
	PrivKey *id.PrivKey
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	privKey := id.NewPrivKey()
	return Options{
		SyncerOptions:   DefaultSyncerOptions(),
		GossiperOptions: DefaultGossiperOptions(),
		DiscoveryOptions: DefaultDiscoveryOptions(),

		Logger:  logger,
		PrivKey: privKey,
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
