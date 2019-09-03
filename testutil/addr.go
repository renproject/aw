package testutil

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/renproject/aw/protocol"
)

func NewSimpleTCPPeerAddressCodec() protocol.PeerAddressCodec {
	return SimpleTCPPeerAddressCodec{}
}

type SimpleTCPPeerAddressCodec struct {
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

func NewSimpleTCPPeerAddress(id, address, port string) SimpleTCPPeerAddress {
	return SimpleTCPPeerAddress{
		ID:        SimplePeerID(id),
		Nonce:     0,
		IPAddress: address,
		Port:      port,
	}
}

type SimplePeerID string

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
	return peerAddr.Nonce > address.Nonce
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
