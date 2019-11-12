package testutil

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"testing/quick"
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
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	id := SimplePeerID("")
	length := rand.Intn(16) + 1
	for i := 0; i < length; i++ {
		id += SimplePeerID(alphabet[rand.Intn(len(alphabet))])
	}
	return id
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

// RandomAddresses returns a random number of distinct PeerAddresses.
func RandomAddresses() protocol.PeerAddresses {
	length := rand.Intn(16)
	addrs := make(protocol.PeerAddresses, length)
	ids := map[string]struct{}{}
	for i := range addrs {
		var addr protocol.PeerAddress
		for {
			addr = RandomAddress()
			if _, ok := ids[addr.String()]; !ok {
				break
			}
		}
		addrs[i] = addr
		ids[addr.String()] = struct{}{}
	}
	return addrs
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

func Contains(addrs protocol.PeerAddresses, addr protocol.PeerAddress) bool {
	for _, address := range addrs {
		if address.Equal(addr) {
			return true
		}
	}
	return false
}

func NewPeerGroupID() protocol.PeerGroupID {
	stringType := reflect.TypeOf(protocol.PeerGroupID("string"))
	value, ok := quick.Value(stringType, rand.New(rand.NewSource(time.Now().Unix())))
	if !ok {
		panic("unable to construct a random PeerGroupID")
	}
	return value.Interface().(protocol.PeerGroupID)
}
