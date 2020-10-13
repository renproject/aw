package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"reflect"

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

// Data is an alias of a byte slice.
type Data = []byte

// A Message defines all of the information needed to gossip information on the
// wire.
type Message struct {
	// The Version is written and read first. This allows peers to choose their
	// unmarshaling logic used for the rest of the Message, based on this
	// Version.
	Version Version `json:"version"`
	Type    Type    `json:"type"`
	Data    Data    `json:"data"`
}

// Equal compares one Message to another. It returns true if they are equal,
// otherwise it returns false.
func (msg Message) Equal(other *Message) bool {
	return msg.Version == other.Version && msg.Type == other.Type && bytes.Equal(msg.Data, other.Data)
}

// Write the message into an I/O writer.
func (msg Message) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, uint8(msg.Version)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint8(msg.Type)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(msg.Data))); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, msg.Data); err != nil {
		return err
	}
	return nil
}

// Read the message from an I/O writer.
func (msg *Message) Read(r io.Reader) error {
	if err := binary.Read(r, binary.BigEndian, (*uint8)(&msg.Version)); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, (*uint8)(&msg.Type)); err != nil {
		return err
	}
	dataLen := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
		return err
	}
	if dataLen > uint32(surge.MaxBytes) {
		return fmt.Errorf("message length exceeds max bytes")
	}
	msg.Data = make([]byte, dataLen)
	if err := binary.Read(r, binary.BigEndian, &msg.Data); err != nil {
		return err
	}
	return nil
}

type PingV1 struct {
	Addr Address `json:"addr"`
	// TODO: Add information about the maximum number of peers that we would
	// like to know about.
}

// Generate implements the quick.Generator interface.
func (s PingV1) Generate(r *rand.Rand, size int) reflect.Value {
	address := Address{
		Protocol:  uint8(r.Uint32()),
		Value:     string(make([]byte, size)),
		Nonce:     r.Uint64(),
		Signature: [id.SizeHintSignature]byte{},
	}
	rand.Read([]byte(address.Value)[:])
	rand.Read(address.Signature[:])
	return reflect.ValueOf(PingV1{Addr: address})
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

// Generate implements the quick.Generator interface.
func (s PingAckV1) Generate(r *rand.Rand, size int) reflect.Value {
	dataSize := (r.Int() % size) + 1
	n := size / dataSize
	if n == 0 {
		n += 1
	}
	addrs := make([]Address, n)
	for i := range addrs {
		addrs[i] = Address{
			Protocol:  uint8(r.Uint32()),
			Value:     string(make([]byte, size)),
			Nonce:     r.Uint64(),
			Signature: [id.SizeHintSignature]byte{},
		}
		rand.Read([]byte(addrs[i].Value)[:])
		rand.Read(addrs[i].Signature[:])
	}
	return reflect.ValueOf(PingAckV1{Addrs: addrs})
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

// Generate implements the quick.Generator interface.
func (s PushV1) Generate(r *rand.Rand, _ int) reflect.Value {
	push := PushV1{
		Subnet:      id.Hash{},
		ContentHash: id.Hash{},
		ContentType: uint8(r.Uint32()),
	}
	rand.Read(push.Subnet[:])
	rand.Read(push.ContentHash[:])
	return reflect.ValueOf(push)
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

// Generate implements the quick.Generator interface.
func (s PushAckV1) Generate(_ *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(PushAckV1{})
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

// Generate implements the quick.Generator interface.
func (s PullV1) Generate(r *rand.Rand, _ int) reflect.Value {
	pull := PullV1{
		Subnet:      id.Hash{},
		ContentHash: id.Hash{},
		ContentType: uint8(r.Uint32()),
	}
	rand.Read(pull.Subnet[:])
	rand.Read(pull.ContentHash[:])
	return reflect.ValueOf(pull)
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

// Generate implements the quick.Generator interface.
func (s PullAckV1) Generate(r *rand.Rand, size int) reflect.Value {
	pull := PullAckV1{
		Subnet:      id.Hash{},
		ContentHash: id.Hash{},
		ContentType: uint8(r.Uint32()),
		Content:     make([]byte, size),
	}
	rand.Read(pull.Subnet[:])
	rand.Read(pull.ContentHash[:])
	rand.Read(pull.Content[:])
	return reflect.ValueOf(pull)
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
