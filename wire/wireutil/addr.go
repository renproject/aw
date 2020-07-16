package wireutil

import (
	"fmt"
	"math/rand"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// ------
//
// Builder.
//
// ------

type AddressBuilder struct {
	protocol  uint8
	value     string
	nonce     uint64
	signature id.Signature
	r         *rand.Rand
}

func NewAddressBuilder(privKey *id.PrivKey, r *rand.Rand) *AddressBuilder {
	protocol := RandomOkAddrProtocol(r)
	value := RandomOkAddrValue(r)
	nonce := RandomAddrNonce(r)
	signature := RandomOkAddrSignature(protocol, value, nonce, privKey)
	return &AddressBuilder{
		protocol:  protocol,
		value:     value,
		nonce:     nonce,
		signature: signature,
		r:         r,
	}
}

func (builder *AddressBuilder) WithProtocol(protocol uint8) *AddressBuilder {
	builder.protocol = protocol
	return builder
}

func (builder *AddressBuilder) WithValue(value string) *AddressBuilder {
	builder.value = value
	return builder
}

func (builder *AddressBuilder) WithNonce(nonce uint64) *AddressBuilder {
	builder.nonce = nonce
	return builder
}

func (builder *AddressBuilder) WithSignature(signature id.Signature) *AddressBuilder {
	builder.signature = signature
	return builder
}

func (builder *AddressBuilder) Build() wire.Address {
	return wire.Address{
		Protocol:  builder.protocol,
		Value:     builder.value,
		Nonce:     builder.nonce,
		Signature: builder.signature,
	}
}

// ------
//
// Random.
//
// ------

func RandomAddrProtocol(r *rand.Rand) uint8 {
	switch r.Int() % 2 {
	case 0:
		return RandomOkAddrProtocol(r)
	default:
		return RandomBadAddrProtocol(r)
	}
}

func RandomAddrValue(r *rand.Rand) string {
	switch r.Int() % 2 {
	case 0:
		return RandomOkAddrValue(r)
	default:
		return RandomBadAddrValue(r)
	}
}

func RandomAddrNonce(r *rand.Rand) uint64 {
	return r.Uint64()
}

func RandomAddrSignature(protocol uint8, value string, nonce uint64, privKey *id.PrivKey, r *rand.Rand) id.Signature {
	switch r.Int() % 2 {
	case 0:
		return RandomOkAddrSignature(protocol, value, nonce, privKey)
	default:
		return RandomBadAddrSignature(r)
	}
}

func RandomPrivKey() *id.PrivKey {
	return id.NewPrivKey()
}

// ------
//
// Random ok.
//
// ------

func RandomOkAddrProtocol(r *rand.Rand) uint8 {
	return wire.TCP
}

func RandomOkAddrValue(r *rand.Rand) string {
	switch r.Int() % 10 {
	case 0:
		return fmt.Sprintf("127.0.0.1:%v", uint16(r.Int()))
	case 1:
		return fmt.Sprintf("0.0.0.0:%v", uint16(r.Int()))
	default:
		return fmt.Sprintf("%v.%v.%v.%v:%v", uint8(r.Int()), uint8(r.Int()), uint8(r.Int()), uint8(r.Int()), uint16(r.Int()))
	}
}

func RandomOkAddrSignature(protocol uint8, value string, nonce uint64, privKey *id.PrivKey) id.Signature {
	addr := wire.NewUnsignedAddress(protocol, value, nonce)
	if err := addr.Sign(privKey); err != nil {
		panic(err)
	}
	return addr.Signature
}

// ------
//
// Random bad.
//
// ------

func RandomBadAddrProtocol(r *rand.Rand) uint8 {
	for {
		protocol := uint8(r.Int())
		switch protocol {
		case wire.TCP:
			continue
		default:
			return protocol
		}
	}
}

func RandomBadAddrValue(r *rand.Rand) string {
	switch r.Int() % 2 {
	case 0:
		return ""
	default:
		str := make([]byte, r.Int()%1024)
		_, err := rand.Read(str)
		if err != nil {
			panic(err)
		}
		return string(str)
	}
}

func RandomBadAddrSignature(r *rand.Rand) id.Signature {
	switch r.Int() % 10 {
	case 0:
		return id.Signature{}
	case 1:
		return id.Signature{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	default:
		sig := id.Signature{}
		_, err := rand.Read(sig[:])
		if err != nil {
			panic(err)
		}
		return sig
	}
}
