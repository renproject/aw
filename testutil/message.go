package testutil

import (
	"math"
	"math/rand"

	"github.com/renproject/aw/protocol"
)

func InvalidMessageVersion() protocol.MessageVersion {
	version := protocol.V1
	for version == protocol.V1 {
		version = protocol.MessageVersion(rand.Intn(math.MaxUint16))
	}
	return version
}

func InvalidMessageVariant() protocol.MessageVariant {
	variant := protocol.Ping
	for protocol.ValidateMessageVariant(variant) == nil {
		variant = protocol.MessageVariant(rand.Intn(math.MaxUint16))
	}
	return variant
}

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
	groupID := protocol.NilPeerGroupID
	if variant == protocol.Multicast || variant == protocol.Broadcast {
		groupID = RandomPeerGroupID()
	}
	return protocol.Message{
		Length:  protocol.MessageLength(8 + len(body)),
		Version: version,
		Variant: variant,
		GroupID: groupID,
		Body:    body,
	}
}
