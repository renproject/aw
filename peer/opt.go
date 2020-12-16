package peer

import (
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type Options struct {
	Logger    *zap.Logger
	PrivKey   *id.PrivKey
	Callbacks Callbacks
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	privKey := id.NewPrivKey()
	return Options{
		Logger:  logger,
		PrivKey: privKey,
		Callbacks: Callbacks{
			DefaultDidReceiveMessage,
		},
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

func (opts Options) WithCallbacks(cb Callbacks) Options {
	opts.Callbacks = cb
	return opts
}
