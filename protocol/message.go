package protocol

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type MessageSender chan<- MessageOnTheWire

type MessageReceiver <-chan MessageOnTheWire

type MessageOnTheWire struct {
	From    net.Addr
	To      net.Addr
	Message Message
}

type MessageReceive struct {
	Message Message
}

type MessageLength uint32

type MessageVersion uint16

const (
	V1 = MessageVersion(1)
)

func (version MessageVersion) String() string {
	switch version {
	case V1:
		return "v1"
	default:
		panic(newErrMessageVersionIsNotSupported(version))
	}
}

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
		panic(newErrMessageVariantIsNotSupported(variant))
	}
}

type MessageHash []byte

func (hash MessageHash) String() string {
	return base64.StdEncoding.EncodeToString(hash)
}

type MessageBody []byte

func (body MessageBody) String() string {
	return base64.StdEncoding.EncodeToString(body)
}

type Message struct {
	Length  MessageLength
	Version MessageVersion
	Variant MessageVariant
	Body    MessageBody
}

func NewMessage(version MessageVersion, variant MessageVariant, body MessageBody) Message {
	switch version {
	case V1:
	default:
		panic(newErrMessageVersionIsNotSupported(version))
	}
	switch variant {
	case Ping, Pong, Cast, Multicast, Broadcast:
	default:
		panic(newErrMessageVariantIsNotSupported(variant))
	}
	if body == nil {
		body = make(MessageBody, 0)
	}

	return Message{
		Length:  MessageLength(32 + len(body)),
		Version: version,
		Variant: variant,
		Body:    body,
	}
}

func (message Message) Hash() MessageHash {
	// FIXME: Return SHA3 hash.
	return nil
}

func (message Message) Write(writer io.Writer) error {
	if message.Length < 32 {
		return newErrMessageLengthIsTooLow(message.Length)
	}
	switch message.Version {
	case V1:
	default:
		return newErrMessageVersionIsNotSupported(message.Version)
	}
	switch message.Variant {
	case Ping, Pong, Cast, Multicast, Broadcast:
	default:
		return newErrMessageVariantIsNotSupported(message.Variant)
	}

	if err := binary.Write(writer, binary.LittleEndian, message.Length); err != nil {
		return fmt.Errorf("error marshaling message length=%v: %v", message.Length, err)
	}
	if err := binary.Write(writer, binary.LittleEndian, message.Version); err != nil {
		return fmt.Errorf("error marshaling message version=%v: %v", message.Version, err)
	}
	if err := binary.Write(writer, binary.LittleEndian, message.Variant); err != nil {
		return fmt.Errorf("error marshaling message variant=%v: %v", message.Variant, err)
	}
	if err := binary.Write(writer, binary.LittleEndian, message.Body); err != nil {
		return fmt.Errorf("error marshaling message body: %v", err)
	}
	return nil
}

func (message *Message) Read(reader io.Reader) error {
	if err := binary.Read(reader, binary.LittleEndian, &message.Length); err != nil {
		return fmt.Errorf("error unmarshaling message length: %v", err)
	}
	if message.Length < 32 {
		return newErrMessageLengthIsTooLow(message.Length)
	}

	if err := binary.Read(reader, binary.LittleEndian, &message.Version); err != nil {
		return fmt.Errorf("error unmarshaling message version: %v", err)
	}
	switch message.Version {
	case V1:
	default:
		return newErrMessageVersionIsNotSupported(message.Version)
	}

	if err := binary.Read(reader, binary.LittleEndian, &message.Variant); err != nil {
		return fmt.Errorf("error unmarshaling message variant: %v", err)
	}
	switch message.Variant {
	case Ping, Pong, Cast, Multicast, Broadcast:
	default:
		return newErrMessageVariantIsNotSupported(message.Variant)
	}

	message.Body = make(MessageBody, message.Length-32)
	if err := binary.Read(reader, binary.LittleEndian, message.Body); err != nil {
		return fmt.Errorf("error unmarshaling message body: %v", err)
	}
	return nil
}

type ErrMessageLengthIsTooLow struct {
	error
	Length MessageLength
}

func newErrMessageLengthIsTooLow(length MessageLength) error {
	return ErrMessageLengthIsTooLow{
		error:  fmt.Errorf("message length=%d is too low", length),
		Length: length,
	}
}

type ErrMessageVersionIsNotSupported struct {
	error
	Version MessageVersion
}

func newErrMessageVersionIsNotSupported(version MessageVersion) error {
	return ErrMessageVersionIsNotSupported{
		error:   fmt.Errorf("message version=%d is not supported", version),
		Version: version,
	}
}

type ErrMessageVariantIsNotSupported struct {
	error
	Variant MessageVariant
}

func newErrMessageVariantIsNotSupported(variant MessageVariant) error {
	return ErrMessageVariantIsNotSupported{
		error:   fmt.Errorf("message variant=%d is not supported", variant),
		Variant: variant,
	}
}
