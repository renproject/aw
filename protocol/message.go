package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/renproject/id"
)

// Message we trying to send on the wire.
type MessageOnTheWire struct {
	To      PeerID
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
	Body    MessageBody
}

// NewMessage returns a new message with given version, variant and body.
func NewMessage(version MessageVersion, variant MessageVariant, body MessageBody) Message {
	if err := ValidateMessageVersion(version); err != nil {
		panic(err)
	}
	if err := ValidateMessageVariant(variant); err != nil {
		panic(err)
	}
	if body == nil {
		body = make(MessageBody, 0)
	}
	return Message{
		Length:  MessageLength(8 + len(body)),
		Version: version,
		Variant: variant,
		Body:    body,
	}
}

// Hash returns the hash of the message.
func (message Message) Hash() id.Hash {
	data, err := message.MarshalBinary()
	if err != nil {
		panic(fmt.Errorf("invariant violation: sha3 hash of bad message: %v", err))
	}
	return sha256.Sum256(data)
}

// ReadMessage reads bytes from a io.Reader and unmarshals it to a message.
func ReadMessage(reader io.Reader) (Message, error) {
	var message Message

	// Read the message length
	if err := binary.Read(reader, binary.LittleEndian, &message.Length); err != nil {
		return message, fmt.Errorf("error unmarshaling message length: %v", err)
	}
	if message.Length < 8 {
		return message, NewErrMessageLengthIsTooLow(message.Length)
	}

	// Read the message version
	if err := binary.Read(reader, binary.LittleEndian, &message.Version); err != nil {
		return message, fmt.Errorf("error unmarshaling message version: %v", err)
	}
	if err := ValidateMessageVersion(message.Version); err != nil {
		return message, err
	}

	// Read the message variant
	if err := binary.Read(reader, binary.LittleEndian, &message.Variant); err != nil {
		return message, fmt.Errorf("error unmarshaling message variant: %v", err)
	}
	if err := ValidateMessageVariant(message.Variant); err != nil {
		return message, err
	}

	// Read the message body.
	message.Body = make(MessageBody, message.Length-8)
	if err := binary.Read(reader, binary.LittleEndian, message.Body); err != nil {
		return message, fmt.Errorf("error unmarshaling message body: %v", err)
	}
	return message, nil
}
