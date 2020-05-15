package handshake

import (
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
)

var (
	DefaultTimeout = 30 * time.Second
	DefaultFilter  = Filter(nil)
)

type Options struct {
	Logger  logrus.FieldLogger
	PrivKey *ecdsa.PrivateKey
	Timeout time.Duration
	Filter  Filter
}

func DefaultOptions() Options {
	privKey, err := crypto.GenerateKey()
	if err != nil {
		panic(fmt.Errorf("generating privkey: %v", err))
	}
	return Options{
		Logger: logrus.New().
			WithField("lib", "airwave").
			WithField("pkg", "handshake").
			WithField("com", "handshaker"),
		PrivKey: privKey,
		Timeout: DefaultTimeout,
		Filter:  DefaultFilter,
	}
}

func (opts Options) WithLogger(logger logrus.FieldLogger) Options {
	opts.Logger = logger
	return opts
}

func (opts Options) WithPrivKey(privKey *ecdsa.PrivateKey) Options {
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
