package testutil

import (
	"math/rand"

	"github.com/renproject/aw/protocol"
)

func RandomBytes(length int) []byte {
	slice := make([]byte, length)
	_, err := rand.Read(slice)
	if err != nil {
		panic(err)
	}
	return slice
}

func RandomMessageBody() []byte {
	length := rand.Intn(256)
	return RandomBytes(length)
}

func RandomMessageVariant() protocol.MessageVariant {
	allVariants := []protocol.MessageVariant{
		protocol.Ping,
		protocol.Pong,
		protocol.Cast,
		protocol.Multicast,
		protocol.Broadcast,
	}
	return allVariants[rand.Intn(len(allVariants))]
}

func RandomMessage(version protocol.MessageVersion, variant protocol.MessageVariant) protocol.Message {
	body := RandomMessageBody()
	return protocol.Message{
		Length:  protocol.MessageLength(8 + len(body)),
		Version: version,
		Variant: variant,
		Body:    body,
	}
}
