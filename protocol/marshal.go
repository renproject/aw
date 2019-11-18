package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// MarshalBinary implements `BinaryMarshaler` interface.
func (message Message) MarshalBinary() ([]byte, error) {

	// Validate message length, version and variant.
	if err := ValidateMessageLength(message.Length, message.Variant); err != nil {
		return nil, err
	}
	if err := ValidateMessageVersion(message.Version); err != nil {
		return nil, err
	}
	if err := ValidateMessageVariant(message.Variant); err != nil {
		return nil, err
	}

	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, message.Length); err != nil {
		return nil, fmt.Errorf("error marshaling message length=%v: %v", message.Length, err)
	}
	if err := binary.Write(buffer, binary.LittleEndian, message.Version); err != nil {
		return nil, fmt.Errorf("error marshaling message version=%v: %v", message.Version, err)
	}
	if err := binary.Write(buffer, binary.LittleEndian, message.Variant); err != nil {
		return nil, fmt.Errorf("error marshaling message variant=%v: %v", message.Variant, err)
	}
	if message.Variant == Broadcast || message.Variant == Multicast {
		if err := binary.Write(buffer, binary.LittleEndian, message.GroupID); err != nil {
			return nil, fmt.Errorf("error marshaling message group id=%v: %v", message.GroupID, err)
		}
	}
	if err := binary.Write(buffer, binary.LittleEndian, message.Body); err != nil {
		return nil, fmt.Errorf("error marshaling message body: %v", err)
	}
	return buffer.Bytes(), nil
}

// UnmarshalBinary implements `BinaryUnmarshaler` interface.
func (message *Message) UnmarshalBinary(data []byte) error {
	return message.UnmarshalReader(bytes.NewBuffer(data))
}

// UnmarshalReader reads bytes from an `io.Reader` and unmarshals them into
// itself.
func (message *Message) UnmarshalReader(reader io.Reader) error {
	// Read the message length
	if err := binary.Read(reader, binary.LittleEndian, &message.Length); err != nil {
		return fmt.Errorf("error unmarshaling message length: %v", err)
	}

	// Read the message version
	if err := binary.Read(reader, binary.LittleEndian, &message.Version); err != nil {
		return fmt.Errorf("error unmarshaling message version: %v", err)
	}
	if err := ValidateMessageVersion(message.Version); err != nil {
		return err
	}

	// Read the message variant
	if err := binary.Read(reader, binary.LittleEndian, &message.Variant); err != nil {
		return fmt.Errorf("error unmarshaling message variant: %v", err)
	}
	if err := ValidateMessageVariant(message.Variant); err != nil {
		return err
	}

	// Validate the message length
	if err := ValidateMessageLength(message.Length, message.Variant); err != nil {
		return err
	}

	// Read the group ID if the message is a Broadcast or a Multicast
	length := 8
	if message.Variant == Broadcast || message.Variant == Multicast {
		length = 40
		if err := binary.Read(reader, binary.LittleEndian, &message.GroupID); err != nil {
			return fmt.Errorf("error unmarshaling message group id: %v", err)
		}
	}

	// Read the message body.
	message.Body = make(MessageBody, int(message.Length)-length)
	if err := binary.Read(reader, binary.LittleEndian, message.Body); err != nil {
		return fmt.Errorf("error unmarshaling message body: %v", err)
	}
	return nil
}
