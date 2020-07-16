package transport

import (
	"github.com/renproject/aw/tcp"
	"go.uber.org/zap"
)

type TCPOptions struct {
}

type WSOptions struct {
	tcp.ClientOptions
	tcp.ServerOptions
}

type Options struct {
	Logger        *zap.Logger
	TCPClientOpts tcp.ClientOptions
	TCPServerOpts tcp.ServerOptions
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return Options{
		Logger:        logger,
		TCPClientOpts: tcp.DefaultClientOptions(),
		TCPServerOpts: tcp.DefaultServerOptions(),
	}
}

func (opts Options) WithLogger(logger *zap.Logger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithTCPClientOptions(clientOpts tcp.ClientOptions) Options {
	opts.TCPClientOpts = clientOpts
	return opts
}

func (opts Options) WithTCPServerOptions(serverOpts tcp.ServerOptions) Options {
	opts.TCPServerOpts = serverOpts
	return opts
}
