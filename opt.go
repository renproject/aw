package aw

import "github.com/sirupsen/logrus"

type Options struct {
	Logger logrus.FieldLogger
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "aw").
			WithField("com", "node"),
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
	opts.Logger = logger
	return opts
}
