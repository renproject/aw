package wire

import (
	"fmt"
	"net"

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
	MsgTypeSend    = uint16(4)
	MsgTypePing    = uint16(5)
	MsgTypePingAck = uint16(6)
)

// Msg defines the low-level message structure that is sent on-the-wire between
// peers.
type Msg struct {
	Version  uint16  `json:"version"`
	Type     uint16  `json:"type"`
	To       id.Hash `json:"to"`
	Data     []byte  `json:"data"`
	SyncData []byte  `json:"syncData"`
}

// Packet defines a struct that captures the incoming message and the corresponding IP address
type Packet struct {
	Msg    Msg
	IPAddr net.Addr
}

// SizeHint returns the number of bytes required to represent a Msg in binary.
func (msg Msg) SizeHint() int {
	return surge.SizeHintU16 +
		surge.SizeHintU16 +
		id.SizeHintHash +
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
	buf, rem, err = surge.Marshal(msg.To, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal to: %v", err)
	}
	buf, rem, err = surge.MarshalBytes(msg.Data, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal data: %v", err)
	}
	return buf, rem, err
}

// Unmarshal a Msg from binary.
func (msg *Msg) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := surge.UnmarshalU16(&msg.Version, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal version: %v", err)
	}
	buf, rem, err = surge.UnmarshalU16(&msg.Type, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal type: %v", err)
	}
	buf, rem, err = surge.Unmarshal(&msg.To, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal to: %v", err)
	}
	buf, rem, err = surge.Unmarshal(&msg.Data, buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal data: %v", err)
	}
	return buf, rem, err
}
