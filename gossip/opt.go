package gossip

import "github.com/sirupsen/logrus"

type Options struct {
	Logger logrus.FieldLogger

	Alpha int
	Bias  float64
}

func DefaultOptions() Options {
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "gossip").
			WithField("com", "gossiper"),
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
	opts.Logger = logger
	return opts
}
