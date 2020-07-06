package wire

import (
	"bytes"
	"fmt"
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

// SizeHint returns the number of bytes required to represent this Message in
// binary.
func (msg Message) SizeHint() int {
	return surge.SizeHint(msg.Version) +
		surge.SizeHint(msg.Type) +
		surge.SizeHint(msg.Data)
}

// Marshal this Message into binary.
func (msg Message) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, uint8(msg.Version), m)
	if err != nil {
		return m, fmt.Errorf("marshaling version: %v", err)
	}
	m, err = surge.Marshal(w, uint8(msg.Type), m)
	if err != nil {
		return m, fmt.Errorf("marshaling variant: %v", err)
	}
	m, err = surge.Marshal(w, msg.Data, m)
	if err != nil {
		return m, fmt.Errorf("marshaling data: %v", err)
	}
	return m, nil
}

// Unmarshal from binary into this Message.
func (msg *Message) Unmarshal(r io.Reader, m int) (int, error) {
	m, err := surge.Unmarshal(r, (*uint8)(&msg.Version), m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling version: %v", err)
	}
	m, err = surge.Unmarshal(r, (*uint8)(&msg.Type), m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling variant: %v", err)
	}
	m, err = surge.Unmarshal(r, &msg.Data, m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling data: %v", err)
	}
	return m, nil
}

type PingV1 struct {
	Addr Address `json:"addr"`
	// TODO: Add information about the maximum number of peers that we would
	// like to know about.
}

func (ping PingV1) SizeHint() int {
	return surge.SizeHint(ping.Addr)
}

func (ping PingV1) Marshal(w io.Writer, m int) (int, error) {
	return surge.Marshal(w, ping.Addr, m)
}

func (ping *PingV1) Unmarshal(r io.Reader, m int) (int, error) {
	return surge.Unmarshal(r, &ping.Addr, m)
}

type PingAckV1 struct {
	Addrs []Address `json:"addrs"`
}

func (pingAck PingAckV1) SizeHint() int {
	return surge.SizeHint(pingAck.Addrs)
}

func (pingAck PingAckV1) Marshal(w io.Writer, m int) (int, error) {
	return surge.Marshal(w, pingAck.Addrs, m)
}

func (pingAck *PingAckV1) Unmarshal(r io.Reader, m int) (int, error) {
	return surge.Unmarshal(r, &pingAck.Addrs, m)
}

type PushV1 struct {
	Subnet      id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	ContentHash id.Hash `json:"hash"`
	ContentType uint8   `json:"type"`
}

func (push PushV1) SizeHint() int {
	return surge.SizeHint(push.Subnet) + surge.SizeHint(push.ContentHash)
}

func (push PushV1) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, push.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, push.ContentHash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, push.ContentType, m)
	if err != nil {
		return m, err
	}
	return m, nil
}

func (push *PushV1) Unmarshal(r io.Reader, m int) (int, error) {
	m, err := surge.Unmarshal(r, &push.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &push.ContentHash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &push.ContentType, m)
	if err != nil {
		return m, err
	}
	return m, nil
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
	return surge.SizeHint(pull.Subnet) + surge.SizeHint(pull.ContentHash)
}

func (pull PullV1) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, pull.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pull.ContentHash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pull.ContentType, m)
	if err != nil {
		return m, err
	}
	return m, nil
}

func (pull *PullV1) Unmarshal(r io.Reader, m int) (int, error) {
	m, err := surge.Unmarshal(r, &pull.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &pull.ContentHash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &pull.ContentType, m)
	if err != nil {
		return m, err
	}
	return m, nil
}

type PullAckV1 struct {
	Subnet      id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	ContentHash id.Hash `json:"hash"`   // TODO: Remove the hash? This should be inferrable (and would need to be checked anyway) against the content. Although, it is nice as a checksum.
	ContentType uint8   `json:"type"`
	Content     []byte  `json:"content"`
}

func (pullAck PullAckV1) SizeHint() int {
	return surge.SizeHint(pullAck.Subnet) + surge.SizeHint(pullAck.ContentHash) + surge.SizeHint(pullAck.Content)
}

func (pullAck PullAckV1) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, pullAck.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pullAck.ContentHash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pullAck.ContentType, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pullAck.Content, m)
	if err != nil {
		return m, err
	}
	return m, nil
}

func (pullAck *PullAckV1) Unmarshal(r io.Reader, m int) (int, error) {
	m, err := surge.Unmarshal(r, &pullAck.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &pullAck.ContentHash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &pullAck.ContentType, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &pullAck.Content, m)
	if err != nil {
		return m, err
	}
	return m, nil
}
