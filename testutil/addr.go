package testutil

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/renproject/aw/protocol"
)

func init() {
	rand.Seed(time.Now().Unix())
}

type SimpleTCPPeerAddressCodec struct {
}

func NewSimpleTCPPeerAddressCodec() protocol.PeerAddressCodec {
	return SimpleTCPPeerAddressCodec{}
}

func (codec SimpleTCPPeerAddressCodec) Encode(peerAddress protocol.PeerAddress) ([]byte, error) {
	address, ok := peerAddress.(SimpleTCPPeerAddress)
	if !ok {
		return nil, fmt.Errorf("unsupported peer address of type: %T", peerAddress)
	}
	return json.Marshal(address)
}

func (codec SimpleTCPPeerAddressCodec) Decode(peerAddress []byte) (protocol.PeerAddress, error) {
	address := SimpleTCPPeerAddress{}
	if err := json.Unmarshal(peerAddress, &address); err != nil {
		return nil, err
	}
	return address, nil
}

type SimplePeerIDCodec struct {
}

func (codec SimplePeerIDCodec) Encode(id protocol.PeerID) ([]byte, error) {
	peerID, ok := id.(SimplePeerID)
	if !ok {
		return nil, fmt.Errorf("unsupported peer peerID of type: %T", id)
	}
	return json.Marshal(peerID)
}

func (codec SimplePeerIDCodec) Decode(data []byte) (protocol.PeerID, error) {
	var peerID SimplePeerID
	if err := json.Unmarshal(data, &peerID); err != nil {
		return nil, err
	}
	return peerID, nil
}

type SimplePeerID string

func RandomPeerID() protocol.PeerID {
	return SimplePeerID(RandomString())
}

func RandomPeerGroupID() protocol.PeerGroupID {
	id := protocol.PeerGroupID{}
	for id.Equal(protocol.NilPeerGroupID) {
		_, err := rand.Read(id[:])
		if err != nil {
			panic(fmt.Sprintf("cannot create random id, err = %v", err))
		}
	}
	return id
}

func RandomString() string {
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := rand.Intn(16) + 1
	str := make([]byte, length)
	for i := 0; i < length; i++ {
		str[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(str)
}

// RandomAddresses returns a random number of distinct PeerAddresses.
func RandomAddresses() protocol.PeerAddresses {
	length := rand.Intn(16)
	addrs := make(protocol.PeerAddresses, length)
	seenAddrs := map[string]struct{}{}
	seenIds := map[string]struct{}{}
	for i := range addrs {
		var addr protocol.PeerAddress
		for {
			addr = RandomAddress()
			if _, ok := seenAddrs[addr.String()]; !ok {
				if _, ok := seenIds[addr.PeerID().String()]; !ok {
					break
				}
			}
		}
		addrs[i] = addr
		seenAddrs[addr.String()] = struct{}{}
		seenIds[addr.PeerID().String()] = struct{}{}
	}
	return addrs
}

func FromAddressesToIDs(addrs protocol.PeerAddresses) protocol.PeerIDs {
	ids := make([]protocol.PeerID, len(addrs))
	for i := range addrs {
		if addrs[i] != nil {
			ids[i] = addrs[i].PeerID()
		}
	}
	return ids
}

// RandomPeerIDs returns a random number of distinct PeerID
func RandomPeerIDs() protocol.PeerIDs {
	length := rand.Intn(16)
	ids := make(protocol.PeerIDs, length)
	distinct := map[string]struct{}{}
	for i := range ids {
		var id protocol.PeerID
		for {
			id = RandomPeerID()
			if _, ok := distinct[id.String()]; !ok {
				break
			}
		}
		ids[i] = id
		distinct[id.String()] = struct{}{}
	}
	return ids
}

func (peerID SimplePeerID) String() string {
	return string(peerID)
}

func (peerID SimplePeerID) Equal(id protocol.PeerID) bool {
	return peerID.String() == id.String()
}

type SimpleTCPPeerAddress struct {
	ID        SimplePeerID `json:"id"`
	Nonce     int64        `json:"nonce"`
	IPAddress string       `json:"ipAddress"`
	Port      string       `json:"port"`
}

func NewSimpleTCPPeerAddress(id, address, port string) SimpleTCPPeerAddress {
	return SimpleTCPPeerAddress{
		ID:        SimplePeerID(id),
		Nonce:     0,
		IPAddress: address,
		Port:      port,
	}
}

func RandomAddress() SimpleTCPPeerAddress {
	id := RandomPeerID()
	ip1 := rand.Intn(128)
	ip2 := rand.Intn(256)
	ip3 := rand.Intn(256)
	ip4 := rand.Intn(256)
	ip := fmt.Sprintf("%v.%v.%v.%v", ip1, ip2, ip3, ip4)
	port := fmt.Sprintf("%v", rand.Intn(65536))
	return NewSimpleTCPPeerAddress(id.String(), ip, port)
}

func (address SimpleTCPPeerAddress) String() string {
	return fmt.Sprintf("/tcp/%s/port/%s/id/%s/nonce/%d", address.IPAddress, address.Port, address.ID.String(), address.Nonce)
}

func (address SimpleTCPPeerAddress) Equal(peerAddr protocol.PeerAddress) bool {
	return address.String() == peerAddr.String()
}

func (address SimpleTCPPeerAddress) PeerID() protocol.PeerID {
	return address.ID
}

func (address SimpleTCPPeerAddress) NetworkAddress() net.Addr {
	netAddress, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", address.IPAddress, address.Port))
	if err != nil {
		return nil
	}
	return netAddress
}

func (address SimpleTCPPeerAddress) IsNewer(peerAddress protocol.PeerAddress) bool {
	peerAddr, ok := peerAddress.(SimpleTCPPeerAddress)
	if !ok {
		return false
	}
	return address.Nonce > peerAddr.Nonce
}

func Remove(addrs protocol.PeerAddresses, i int) protocol.PeerAddresses {
	clonedAddrs := ClonePeerAddresses(addrs)
	return append(clonedAddrs[:i], clonedAddrs[i+1:]...)
}

func ClonePeerAddresses(addrs protocol.PeerAddresses) protocol.PeerAddresses {
	clonedAddrs := make(protocol.PeerAddresses, len(addrs))
	for i := range clonedAddrs {
		clonedAddrs[i] = addrs[i]
	}
	return clonedAddrs
}
