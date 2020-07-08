package wire

import (
	"bytes"
	"io"

	"github.com/renproject/id"
	"github.com/renproject/surge"
)

// Version of a message.
type Version uint8

// Enumeration of all supported versions.
const (
	V1 = Version(1)
)

// Type of a message. This is the high-level type of the message, used by the
// P2P network itself, not the content type of the message.
type Type uint8

// Enumeration of all supported types.
const (
	// These values are reserved for forwards compatibility.
	Reserved0 = Type(0)
	Reserved1 = Type(1)

	// Ping and PingAck are used for Address discovery. Ping is used to notify
	// other peers about our Address, and PingAck is sent as a response to
	// notify us about their Address. This allows for quick Address discovery
	// during boot, because we do not need to wait for other peers to begin
	// pinging (we can begin pinging, and expect to receive PingAcks).
	Ping    = Type(2)
	PingAck = Type(3)

	// Push and PushAck are used for pushing references to new data to other
	// peers in the network. Push notifies other peers about the new data (but
	// does not actually contain the data), and PushAck is sent as a response to
	// acknowledge the existence of this new data.
	Push    = Type(4)
	PushAck = Type(5)

	// Pull and PullAck are used for pulling data from peers in the network.
	// After receiving a Push, we can decide if we need the associated data, and
	// send out a Pull for that data. In response, we will receive PullAcks that
	// contain the data. This allows for efficient gossiping, because we can
	// ensure that we only Pull new data once (and this saves a lot of
	// bandwidth).
	Pull    = Type(6)
	PullAck = Type(7)
)

// A Message defines all of the information needed to gossip information on the
// wire.
type Message struct {
	// The Version is written and read first. This allows peers to choose their
	// unmarshaling logic used for the rest of the Message, based on this
	// Version.
	Version Version `json:"version"`
	Type    Type    `json:"type"`
	Data    []byte  `json:"data"`
}

// Equal compares one Message to another. It returns true if they are equal,
// otherwise it returns false.
func (msg Message) Equal(other *Message) bool {
	return msg.Version == other.Version && msg.Type == other.Type && bytes.Equal(msg.Data, other.Data)
}

// Marshal this Message into binary.
func (msg Message) Marshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	if buf, rem, err = surge.MarshalU8(uint8(msg.Version), buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = surge.MarshalU8(uint8(msg.Type), buf, rem); err != nil {
		return buf, rem, err
	}
	return surge.MarshalBytes(msg.Data, buf, rem)
}

// Unmarshal from binary into this Message.
func (msg *Message) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	buf, rem, err = surge.UnmarshalU8((*uint8)(&msg.Version), buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = surge.UnmarshalU8((*uint8)(&msg.Type), buf, rem)
	if err != nil {
		return buf, rem, err
	}
	return surge.UnmarshalBytes(&msg.Data, buf, rem)
}

type PingV1 struct {
	Addr Address `json:"addr"`
	// TODO: Add information about the maximum number of peers that we would
	// like to know about.
}

func (ping PingV1) SizeHint() int {
	return ping.Addr.SizeHint()
}

func (ping PingV1) Marshal(buf []byte, rem int) ([]byte, int, error) {
	return ping.Addr.Marshal(buf, rem)
}

func (ping *PingV1) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	return ping.Addr.Unmarshal(buf, rem)
}

type PingAckV1 struct {
	Addrs []Address `json:"addrs"`
}

func (pingAck PingAckV1) SizeHint() int {
	return surge.SizeHint(pingAck.Addrs)
}

func (pingAck PingAckV1) Marshal(buf []byte, rem int) ([]byte, int, error) {
	return surge.Marshal(pingAck.Addrs, buf, rem)
}

func (pingAck *PingAckV1) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	return surge.Unmarshal(&pingAck.Addrs, buf, rem)
}

type PushV1 struct {
	Subnet      id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	ContentHash id.Hash `json:"hash"`
	ContentType uint8   `json:"type"`
}

func (push PushV1) SizeHint() int {
	return push.Subnet.SizeHint() + push.ContentHash.SizeHint() + surge.SizeHintU8
}

func (push PushV1) Marshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	if buf, rem, err = push.Subnet.Marshal(buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = push.ContentHash.Marshal(buf, rem); err != nil {
		return buf, rem, err
	}
	return surge.MarshalU8(push.ContentType, buf, rem)
}

func (push *PushV1) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	buf, rem, err = push.Subnet.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = push.ContentHash.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, err
	}
	return surge.UnmarshalU8(&push.ContentType, buf, rem)
}

type PushAckV1 struct {
}

func (push PushAckV1) SizeHint() int {
	return 0
}

func (push PushAckV1) Marshal(w io.Writer, m int) (int, error) {
	return m, nil
}

func (push *PushAckV1) Unmarshal(r io.Reader, m int) (int, error) {
	return m, nil
}

type PullV1 struct {
	Subnet      id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	ContentHash id.Hash `json:"hash"`
	ContentType uint8   `json:"type"`
}

func (pull PullV1) SizeHint() int {
	return pull.Subnet.SizeHint() + pull.ContentHash.SizeHint() + surge.SizeHintU8
}

func (pull PullV1) Marshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	if buf, rem, err = pull.Subnet.Marshal(buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = pull.ContentHash.Marshal(buf, rem); err != nil {
		return buf, rem, err
	}
	return surge.MarshalU8(pull.ContentType, buf, rem)
}

func (pull *PullV1) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	buf, rem, err = pull.Subnet.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = pull.ContentHash.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, err
	}
	return surge.UnmarshalU8(&pull.ContentType, buf, rem)
}

type PullAckV1 struct {
	Subnet      id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	ContentHash id.Hash `json:"hash"`   // TODO: Remove the hash? This should be inferrable (and would need to be checked anyway) against the content. Although, it is nice as a checksum.
	ContentType uint8   `json:"type"`
	Content     []byte  `json:"content"`
}

func (pullAck PullAckV1) SizeHint() int {
	return pullAck.Subnet.SizeHint() +
		pullAck.ContentHash.SizeHint() +
		surge.SizeHintU8 +
		surge.SizeHintBytes(pullAck.Content)
}

func (pullAck PullAckV1) Marshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	if buf, rem, err = pullAck.Subnet.Marshal(buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = pullAck.ContentHash.Marshal(buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = surge.MarshalU8(pullAck.ContentType, buf, rem); err != nil {
		return buf, rem, err
	}
	return surge.MarshalBytes(pullAck.Content, buf, rem)
}

func (pullAck *PullAckV1) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	buf, rem, err = pullAck.Subnet.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = pullAck.ContentHash.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = surge.UnmarshalU8(&pullAck.ContentType, buf, rem)
	if err != nil {
		return buf, rem, err
	}
	return surge.UnmarshalBytes(&pullAck.Content, buf, rem)
}
