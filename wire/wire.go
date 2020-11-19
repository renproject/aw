package wire

import (
	"fmt"
	"github.com/renproject/id"

	"github.com/renproject/surge"
)

// Enumerate all valid MsgVersion values.
const (
	MsgVersion1 = uint16(1)
)

// Enumerate all valid MsgType values.
const (
	MsgTypePush    = uint16(1)
	MsgTypePull    = uint16(2)
	MsgTypeSync    = uint16(3)
	MsgTypeSend = uint16(4)
)

// Msg defines the low-level message structure that is sent on-the-wire between
// peers.
type Msg struct {
	Version uint16 `json:"version"`
	Type    uint16 `json:"type"`
	To 		id.Signatory `json:"to"`
	Data    []byte `json:"data"`
}

// SizeHint returns the number of bytes required to represent a Msg in binary.
func (msg Msg) SizeHint() int {
	return surge.SizeHintU16 +
		surge.SizeHintU16 +
		surge.SizeHintBytes(msg.Data)
}

// Marshal a Msg to binary.
func (msg Msg) Marshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := surge.MarshalU16(msg.Version, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal version: %v", err)
	}
	buf, rem, err = surge.MarshalU16(msg.Type, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal type: %v", err)
	}
	buf, rem, err = surge.MarshalBytes(msg.Data, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal data: %v", err)
	}
	return buf, rem, nil
}

// Unmarshal a Msg from binary.
func (msg *Msg) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := surge.UnmarshalU16(&msg.Version, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal version: %v", err)
	}
	buf, rem, err = surge.UnmarshalU16(&msg.Type, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal type: %v", err)
	}
	buf, rem, err = surge.UnmarshalBytes(&msg.Data, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal data: %v", err)
	}
	return buf, rem, nil
}
