package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/renproject/id"
)

// Message we trying to send on the wire.
type MessageOnTheWire struct {
	To      PeerAddress
	From    PeerID
	Message Message
}

// MessageSender is used for sending MessageOnTheWire.
type MessageSender chan<- MessageOnTheWire

// MessageReceiver is used for reading MessageOnTheWire.
type MessageReceiver <-chan MessageOnTheWire

// MessageOnTheWireResponse is the result of the message sending action.
type MessageOnTheWireResponse struct {
	To      PeerID
	Success bool
}

// MessageResponseSender is used for sending MessageOnTheWireResponse.
type MessageResponseSender chan<- MessageOnTheWireResponse

// MessageResponseSender is used for reading MessageOnTheWireResponse.
type MessageResponseReceiver <-chan MessageOnTheWireResponse

// MessageReceive represents the message read from the stream.
type MessageReceive struct {
	Message Message
}

// MessageLength indicates the length of the entire message.
type MessageLength uint32

// ValidateMessageVersion checks if the length is valid.
func ValidateMessageLength(length MessageLength, variant MessageVariant) error {
	switch variant {
	case Cast, Ping, Pong:
		if length < 8 {
			return NewErrMessageLengthIsTooLow(length)
		}
	case Multicast, Broadcast:
		if length < 40 {
			return NewErrMessageLengthIsTooLow(length)
		}
	default:
		return NewErrMessageVariantIsNotSupported(variant)
	}
	return nil
}

// MessageVersion indicates the version of the message.
type MessageVersion uint16

const (
	V1 = MessageVersion(1)
)

func (version MessageVersion) String() string {
	switch version {
	case V1:
		return "v1"
	default:
		panic(NewErrMessageVersionIsNotSupported(version))
	}
}

// ValidateMessageVersion checks if the given version is supported.
func ValidateMessageVersion(version MessageVersion) error {
	switch version {
	case V1:
		return nil
	default:
		return NewErrMessageVersionIsNotSupported(version)
	}
}

// MessageVariant represents the type of message.
type MessageVariant uint16

const (
	Ping      = MessageVariant(1)
	Pong      = MessageVariant(2)
	Cast      = MessageVariant(3)
	Multicast = MessageVariant(4)
	Broadcast = MessageVariant(5)
)

func (variant MessageVariant) String() string {
	switch variant {
	case Ping:
		return "ping"
	case Pong:
		return "pong"
	case Cast:
		return "cast"
	case Multicast:
		return "multicast"
	case Broadcast:
		return "broadcast"
	default:
		panic(NewErrMessageVariantIsNotSupported(variant))
	}
}

// ValidateMessageVariant checks if the given variant is supported.
func ValidateMessageVariant(variant MessageVariant) error {
	switch variant {
	case Ping, Pong, Cast, Multicast, Broadcast:
		return nil
	default:
		return NewErrMessageVariantIsNotSupported(variant)
	}
}

// MessageBody contains the content of the message.
type MessageBody []byte

// String implements the `Stringer` interface.
func (body MessageBody) String() string {
	return base64.RawStdEncoding.EncodeToString(body)
}

// Message is the object users communicating in the network.
type Message struct {
	Length  MessageLength
	Version MessageVersion
	Variant MessageVariant
	GroupID PeerGroupID
	Body    MessageBody
}

// NewMessage returns a new message with given version, variant and body.
func NewMessage(version MessageVersion, variant MessageVariant, groupID PeerGroupID, body MessageBody) Message {
	if err := ValidateMessageVersion(version); err != nil {
		panic(err)
	}
	if err := ValidateMessageVariant(variant); err != nil {
		panic(err)
	}
	if err := ValidatePeerGroupID(groupID, variant); err != nil {
		panic(err)
	}
	if body == nil {
		body = make(MessageBody, 0)
	}

	length := 8
	if variant == Broadcast || variant == Multicast {
		length = 40
	}

	return Message{
		Length:  MessageLength(length + len(body)),
		Version: version,
		Variant: variant,
		GroupID: groupID,
		Body:    body,
	}
}

// Hash returns the hash of the message.
func (message Message) Hash() id.Hash {
	data, err := message.MarshalBinary()
	if err != nil {
		panic(fmt.Errorf("invariant violation: malformed message: %v", err))
	}
	return sha256.Sum256(data)
}

func ValidatePeerGroupID(groupID PeerGroupID, variant MessageVariant) error {
	if variant != Broadcast && variant != Multicast {
		if groupID != NilPeerGroupID {
			return ErrInvalidPeerGroupID
		}
	}
	return nil
}

// ValidateMessage checks if the given message is valid.
func ValidateMessage(message Message) error {
	if err := ValidateMessageVersion(message.Version); err != nil {
		return err
	}
	if err := ValidateMessageVariant(message.Variant); err != nil {
		return err
	}
	if err := ValidatePeerGroupID(message.GroupID, message.Variant); err != nil {
		return err
	}

	length := 8
	if message.Variant != Broadcast && message.Variant != Multicast {
		length = 40
	}
	if int(message.Length) != length+len(message.Body) {
		return ErrInvalidPeerGroupID
	}

	return nil
}
