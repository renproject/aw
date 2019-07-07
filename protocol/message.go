package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
)

type MessageSender chan<- MessageSend

type MessageSend struct {
	To      net.Addr
	Message Message
}

type MessageReceiver <-chan MessageReceive

type MessageReceive struct {
	From    net.Addr
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
	Cast      = MessageVariant(1)
	Multicast = MessageVariant(2)
	Broadcast = MessageVariant(3)
)

func (variant MessageVariant) String() string {
	switch variant {
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
	case Cast, Multicast, Broadcast:
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

func (message Message) MarshalBinary() ([]byte, error) {
	if message.Length < 32 {
		return nil, newErrMessageLengthIsTooLow(message.Length)
	}
	switch message.Version {
	case V1:
	default:
		return nil, newErrMessageVersionIsNotSupported(message.Version)
	}
	switch message.Variant {
	case Cast, Multicast, Broadcast:
	default:
		return nil, newErrMessageVariantIsNotSupported(message.Variant)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, message.Length); err != nil {
		return nil, fmt.Errorf("error marshaling message length=%v: %v", message.Length, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, message.Version); err != nil {
		return nil, fmt.Errorf("error marshaling message version=%v: %v", message.Version, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, message.Variant); err != nil {
		return nil, fmt.Errorf("error marshaling message variant=%v: %v", message.Variant, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, message.Body); err != nil {
		return nil, fmt.Errorf("error marshaling message body: %v", err)
	}
	return buf.Bytes(), nil
}

func (message *Message) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)

	if err := binary.Read(buf, binary.LittleEndian, &message.Length); err != nil {
		return fmt.Errorf("error unmarshaling message length: %v", err)
	}
	if message.Length < 32 {
		return newErrMessageLengthIsTooLow(message.Length)
	}

	if err := binary.Read(buf, binary.LittleEndian, &message.Version); err != nil {
		return fmt.Errorf("error unmarshaling message version: %v", err)
	}
	switch message.Version {
	case V1:
	default:
		return newErrMessageVersionIsNotSupported(message.Version)
	}

	if err := binary.Read(buf, binary.LittleEndian, &message.Variant); err != nil {
		return fmt.Errorf("error unmarshaling message variant: %v", err)
	}
	switch message.Variant {
	case Cast, Multicast, Broadcast:
	default:
		return newErrMessageVariantIsNotSupported(message.Variant)
	}

	message.Body = make(MessageBody, message.Length-32)
	if err := binary.Read(buf, binary.LittleEndian, message.Body); err != nil {
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
		error:  fmt.Errorf("message length=%v is too low", length),
		Length: length,
	}
}

type ErrMessageVersionIsNotSupported struct {
	error
	Version MessageVersion
}

func newErrMessageVersionIsNotSupported(version MessageVersion) error {
	return ErrMessageVersionIsNotSupported{
		error:   fmt.Errorf("message version=%v is not supported", version),
		Version: version,
	}
}

type ErrMessageVariantIsNotSupported struct {
	error
	Variant MessageVariant
}

func newErrMessageVariantIsNotSupported(variant MessageVariant) error {
	return ErrMessageVariantIsNotSupported{
		error:   fmt.Errorf("message variant=%v is not supported", variant),
		Variant: variant,
	}
}
