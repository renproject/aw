package transport

import (
	"github.com/renproject/aw/tcp"
	"github.com/sirupsen/logrus"
)

type Options struct {
	Logger logrus.FieldLogger

	TCPClientOpts tcp.ClientOptions
	TCPServerOpts tcp.ServerOptions
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "transport").
			WithField("com", "trans"),

		TCPClientOpts: tcp.DefaultClientOptions(),
		TCPServerOpts: tcp.DefaultServerOptions(),
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithTCPServerOptions(tcpServerOpts tcp.ServerOptions) Options {
	opts.TCPServerOpts = tcpServerOpts
	return opts
}
