package transport

import (
	"github.com/renproject/aw/tcp"
	"github.com/sirupsen/logrus"
)

type TCPOptions struct {
}

type WSOptions struct {
	tcp.ClientOptions
	tcp.ServerOptions
}

type Options struct {
	Logger        logrus.FieldLogger
	TCPClientOpts tcp.ClientOptions
	TCPServerOpts tcp.ServerOptions
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "transport").
			WithField("com", "transport"),
		TCPClientOpts: tcp.DefaultClientOptions(),
		TCPServerOpts: tcp.DefaultServerOptions(),
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
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
