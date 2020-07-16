package wireutil

import (
	"math/rand"

	"github.com/renproject/aw/wire"
	"github.com/renproject/surge"
)

// ------
//
// Builder.
//
// ------

type MessageBuilder struct {
	version wire.Version
	ty      wire.Type
	data    []byte
	r       *rand.Rand
}

func NewMessageBuilder(r *rand.Rand) *MessageBuilder {
	return &MessageBuilder{
		version: RandomOkMessageVersion(r),
		ty:      RandomOkMessageType(r),
		data:    RandomOkMessageData(r),
		r:       r,
	}
}

func (builder *MessageBuilder) WithVersion(version wire.Version) *MessageBuilder {
	builder.version = version
	return builder
}

func (builder *MessageBuilder) WithType(ty wire.Type) *MessageBuilder {
	builder.ty = ty
	return builder
}

func (builder *MessageBuilder) WithData(data []byte) *MessageBuilder {
	builder.data = data
	return builder
}

func (builder *MessageBuilder) Build() wire.Message {
	copied := make([]byte, len(builder.data))
	copy(copied, builder.data)
	return wire.Message{
		Version: builder.version,
		Type:    builder.ty,
		Data:    copied,
	}
}

// ------
//
// Random.
//
// ------

func RandomMessageVersion(r *rand.Rand) wire.Version {
	switch r.Int() % 2 {
	case 0:
		return RandomOkMessageVersion(r)
	default:
		return RandomBadMessageVersion(r)
	}
}

func RandomMessageType(r *rand.Rand) wire.Type {
	switch r.Int() % 2 {
	case 0:
		return RandomOkMessageType(r)
	default:
		return RandomBadMessageType(r)
	}
}

func RandomMessageData(r *rand.Rand) []byte {
	switch r.Int() % 2 {
	case 0:
		return RandomOkMessageData(r)
	default:
		return RandomBadMessageData(r)
	}
}

// ------
//
// Random ok.
//
// ------

func RandomOkMessageVersion(r *rand.Rand) wire.Version {
	return wire.V1
}

func RandomOkMessageType(r *rand.Rand) wire.Type {
	switch r.Int() % 6 {
	case 0:
		return wire.Ping
	case 1:
		return wire.PingAck
	case 2:
		return wire.Push
	case 3:
		return wire.PushAck
	case 4:
		return wire.Pull
	default:
		return wire.PullAck
	}
}

func RandomOkMessageData(r *rand.Rand) []byte {
	switch r.Int() % 10 {
	case 0:
		return []byte{}
	default:
		data := make([]byte, r.Int()%surge.MaxBytes)
		_, err := rand.Read(data)
		if err != nil {
			panic(err)
		}
		return data
	}
}

// ------
//
// Random bad.
//
// ------

func RandomBadMessageVersion(r *rand.Rand) wire.Version {
	for {
		version := wire.Version(r.Int())
		switch version {
		case wire.V1:
			continue
		default:
			return version
		}
	}
}

func RandomBadMessageType(r *rand.Rand) wire.Type {
	for {
		ty := wire.Type(r.Int())
		switch ty {
		case wire.Ping, wire.PingAck, wire.Push, wire.PushAck, wire.Pull, wire.PullAck:
			continue
		default:
			return ty
		}
	}
}

func RandomBadMessageData(r *rand.Rand) []byte {
	return nil
}
