package wire

import (
	"bytes"
	"fmt"
	"io"

	"github.com/renproject/id"
	"github.com/renproject/surge"
)

// Enumeration of all supported versions.
const (
	V1 = uint8(1)
)

// Enumeration of all supported types.
const (
	// These values are reserved for forwards compatibility.
	Reserved0 = uint8(0)
	Reserved1 = uint8(1)

	// Ping and PingAck are used for Address discovery. Ping is used to notify
	// other peers about our Address, and PingAck is sent as a response to
	// notify us about their Address. This allows for quick Address discovery
	// during boot, because we do not need to wait for other peers to begin
	// pinging (we can begin pinging, and expect to receive PingAcks).
	Ping    = uint8(2)
	PingAck = uint8(3)

	// Push and PushAck are used for pushing references to new data to other
	// peers in the network. Push notifies other peers about the new data (but
	// does not actually contain the data), and PushAck is sent as a response to
	// acknowledge the existence of this new data.
	Push    = uint8(4)
	PushAck = uint8(5)

	// Pull and PullAck are used for pulling data from peers in the network.
	// After receiving a Push, we can decide if we need the associated data, and
	// send out a Pull for that data. In response, we will receive PullAcks that
	// contain the data. This allows for efficient gossiping, because we can
	// ensure that we only Pull new data once (and this saves a lot of
	// bandwidth).
	Pull    = uint8(6)
	PullAck = uint8(7)

	// These values are reserved for forwards compatibility.
	Reserved8  = uint8(8)
	Reserved9  = uint8(9)
	Reserved10 = uint8(10)
	Reserved11 = uint8(11)
	Reserved12 = uint8(12)
	Reserved13 = uint8(13)
	Reserved14 = uint8(14)
	Reserved15 = uint8(15)
)

// A Message defines all of the information needed to gossip information on the
// wire.
type Message struct {
	// The Version is written and read first. This allows peers to choose their
	// unmarshaling logic used for the rest of the Message, based on this
	// Version.
	Version uint8  `json:"version"`
	Type    uint8  `json:"type"`
	Data    []byte `json:"data"`
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
	m, err := surge.Marshal(w, msg.Version, m)
	if err != nil {
		return m, fmt.Errorf("marshaling version: %v", err)
	}
	m, err = surge.Marshal(w, msg.Type, m)
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
	m, err := surge.Unmarshal(r, &msg.Version, m)
	if err != nil {
		return m, fmt.Errorf("unmarshaling version: %v", err)
	}
	m, err = surge.Unmarshal(r, &msg.Type, m)
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
	Subnet id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	Type   uint8   `json:"type"`
	Hash   id.Hash `json:"hash"`
}

func (push PushV1) SizeHint() int {
	return surge.SizeHint(push.Subnet) + surge.SizeHint(push.Hash)
}

func (push PushV1) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, push.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, push.Hash, m)
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
	m, err = surge.Unmarshal(r, &push.Hash, m)
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
	Subnet id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	Hash   id.Hash `json:"hash"`
	// TODO: Add information about the type of data that this hash identifies.
	// For example, is it a transaction, a block, or something else?
}

func (pull PullV1) SizeHint() int {
	return surge.SizeHint(pull.Subnet) + surge.SizeHint(pull.Hash)
}

func (pull PullV1) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, pull.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pull.Hash, m)
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
	m, err = surge.Unmarshal(r, &pull.Hash, m)
	if err != nil {
		return m, err
	}
	return m, nil
}

type PullAckV1 struct {
	Subnet  id.Hash `json:"subnet"` // TODO: Remove the subnet? Make it optional?
	Hash    id.Hash `json:"hash"`   // TODO: Remove the hash? This should be inferrable (and would need to be checked anyway) against the content. Although, it is nice as a checksum.
	Type    uint8   `json:"type"`
	Content []byte  `json:"content"`
}

func (pullAck PullAckV1) SizeHint() int {
	return surge.SizeHint(pullAck.Subnet) + surge.SizeHint(pullAck.Hash) + surge.SizeHint(pullAck.Content)
}

func (pullAck PullAckV1) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, pullAck.Subnet, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Marshal(w, pullAck.Hash, m)
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
	m, err = surge.Unmarshal(r, &pullAck.Hash, m)
	if err != nil {
		return m, err
	}
	m, err = surge.Unmarshal(r, &pullAck.Content, m)
	if err != nil {
		return m, err
	}
	return m, nil
}
