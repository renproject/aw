package peer

import (
	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/aw/experiment/transport"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

var (
	DefaultBind    = "localhost"
	DefaultPort    = uint16(3333)
	DefaultEncoder = codec.LengthPrefixEncoder(codec.PlainEncoder)
	DefaultDecoder = codec.LengthPrefixDecoder(codec.PlainDecoder)
)

type Callbacks struct {
	DidReceiveMessage func(remote id.Signatory, msg Message)
}

type Options struct {
	Logger           *zap.Logger
	PrivKey          *id.PrivKey
	Bind             string
	Port             uint16
	ClientHandshake  handshake.Handshake
	ServerHandshake  handshake.Handshake
	Encoder          codec.Encoder
	Decoder          codec.Decoder
	Callbacks        Callbacks
	TransportOptions transport.Options
}

func DefaultOptions() Options {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	privKey := id.NewPrivKey()
	self := id.NewSignatory(privKey.PubKey())
	return Options{
		Logger:          logger,
		PrivKey:         privKey,
		Bind:            DefaultBind,
		Port:            DefaultPort,
		ClientHandshake: handshake.InsecureHandshake(self),
		ServerHandshake: handshake.InsecureHandshake(self),
		Encoder:         DefaultEncoder,
		Decoder:         DefaultDecoder,
		Callbacks: Callbacks{
			DidReceiveMessage: func(id.Signatory, Message) {},
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

func (opts Options) WithBind(bind string) Options {
	opts.Bind = bind
	return opts
}

func (opts Options) WithPort(port uint16) Options {
	opts.Port = port
	return opts
}

func (opts Options) WithHandshake(clientHandshake, serverHandshake handshake.Handshake) Options {
	opts.ClientHandshake = clientHandshake
	opts.ServerHandshake = serverHandshake
	return opts
}

func (opts Options) WithCodec(enc codec.Encoder, dec codec.Decoder) Options {
	opts.Encoder = enc
	opts.Decoder = dec
	return opts
}

func (opts Options) WithCallbacks(cbs Callbacks) Options {
	opts.Callbacks = cbs
	return opts
}

func (opts Options) WithTransportOptions(transportOpts transport.Options) Options {
	opts.TransportOptions = transportOpts
	return opts
}
