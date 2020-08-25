package wire

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

// Protocol values for the different network address protocols that are
// supported.
const (
	UndefinedProtocol = uint8(0)
	TCP               = uint8(1)
	UDP               = uint8(2)
	WebSocket         = uint8(3)
)

// NewAddressHash returns the Hash of an Address for signing by the peer. An
// error is returned when the arguments too large and cannot be marshaled into
// bytes without exceeding memory allocation restrictions.
func NewAddressHash(protocol uint8, value string, nonce uint64) (id.Hash, error) {
	buf := make([]byte, surge.SizeHintU8+surge.SizeHintString(value)+surge.SizeHintU64)
	return NewAddressHashWithBuffer(protocol, value, nonce, buf)
}

// NewAddressHashWithBuffer writes the Hash of an Address into a bytes buffer
// for signing by the peer. An error is returned when the arguments are too
// large and cannot be marshaled into bytes without exceeding memory allocation
// restrictions. This function is useful when doing a lot of hashing, because it
// allows for buffer re-use.
func NewAddressHashWithBuffer(protocol uint8, value string, nonce uint64, data []byte) (id.Hash, error) {
	var err error
	buf := data
	rem := surge.MaxBytes
	if buf, rem, err = surge.MarshalU8(protocol, buf, rem); err != nil {
		return id.Hash{}, err
	}
	if buf, rem, err = surge.MarshalString(value, buf, rem); err != nil {
		return id.Hash{}, err
	}
	if buf, rem, err = surge.MarshalU64(nonce, buf, rem); err != nil {
		return id.Hash{}, err
	}
	return id.Hash(sha256.Sum256(buf)), nil
}

// An Address is a verifiable and expirable network address associated with a
// specific peer. The peer can be verified by checking the Signatory of the peer
// against the Signature in the Address. The Address can be expired by issuing a
// new Address for the same peer, using a later nonce. By convention, nonces are
// interpreted as seconds since UNIX epoch.
type Address struct {
	Protocol  uint8        `json:"protocol"`
	Value     string       `json:"value"`
	Nonce     uint64       `json:"nonce"`
	Signature id.Signature `json:"signature"`
}

// NewUnsignedAddress returns an Address that has an empty signature. The Sign
// method should be called before the returned Address is used.
func NewUnsignedAddress(protocol uint8, value string, nonce uint64) Address {
	return Address{
		Protocol: protocol,
		Value:    value,
		Nonce:    nonce,
	}
}

// SizeHint returns the number of bytes needed to represent this Address in
// binary.
func (addr Address) SizeHint() int {
	return surge.SizeHintU8 +
		surge.SizeHintString(addr.Value) +
		surge.SizeHintU64 +
		addr.Signature.SizeHint()
}

// Marshal this Address into binary.
func (addr Address) Marshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	if buf, rem, err = surge.MarshalU8(addr.Protocol, buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = surge.MarshalString(addr.Value, buf, rem); err != nil {
		return buf, rem, err
	}
	if buf, rem, err = surge.MarshalU64(addr.Nonce, buf, rem); err != nil {
		return buf, rem, err
	}
	return addr.Signature.Marshal(buf, rem)
}

// Unmarshal from binary into this Address.
func (addr *Address) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	var err error
	buf, rem, err = surge.UnmarshalU8(&addr.Protocol, buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = surge.UnmarshalString(&addr.Value, buf, rem)
	if err != nil {
		return buf, rem, err
	}
	buf, rem, err = surge.UnmarshalU64(&addr.Nonce, buf, rem)
	if err != nil {
		return buf, rem, err
	}
	return addr.Signature.Unmarshal(buf, rem)
}

// Sign this Address and set its Signature.
func (addr *Address) Sign(privKey *id.PrivKey) error {
	buf := make([]byte, surge.SizeHintU8+surge.SizeHintString(addr.Value)+surge.SizeHintU64)
	return addr.SignWithBuffer(privKey, buf)
}

// SignWithBuffer will Sign the Address and set its Signature. It uses a Buffer
// for all marshaling to allow for buffer re-use.
func (addr *Address) SignWithBuffer(privKey *id.PrivKey, buf []byte) error {
	hash, err := NewAddressHashWithBuffer(addr.Protocol, addr.Value, addr.Nonce, buf)
	if err != nil {
		return fmt.Errorf("hashing address: %v", err)
	}
	signature, err := crypto.Sign(hash[:], (*ecdsa.PrivateKey)(privKey))
	if err != nil {
		return fmt.Errorf("signing address hash: %v", err)
	}
	if n := copy(addr.Signature[:], signature); n != len(addr.Signature) {
		return fmt.Errorf("copying signature: expected n=%v, got n=%v", len(addr.Signature), n)
	}
	return nil
}

// Verify that the Address was signed by a specific Signatory.
func (addr *Address) Verify(signatory id.Signatory) error {
	buf := make([]byte, surge.SizeHintU8+surge.SizeHintString(addr.Value)+surge.SizeHintU64)
	return addr.VerifyWithBuffer(signatory, buf)
}

// VerifyWithBuffer will verify that the Address was signed by a specific
// Signatory. It uses a Buffer for all marshaling to allow for buffer re-use.
func (addr *Address) VerifyWithBuffer(signatory id.Signatory, buf []byte) error {
	hash, err := NewAddressHashWithBuffer(addr.Protocol, addr.Value, addr.Nonce, buf)
	if err != nil {
		return fmt.Errorf("hashing address: %v", err)
	}
	verifiedPubKey, err := crypto.SigToPub(hash[:], addr.Signature[:])
	if err != nil {
		return fmt.Errorf("identifying address signature: %v", err)
	}
	verifiedSignatory := id.NewSignatory((*id.PubKey)(verifiedPubKey))
	if !signatory.Equal(&verifiedSignatory) {
		return fmt.Errorf("verifying address signatory: expected=%v, got=%v", signatory, verifiedSignatory)
	}
	return nil
}

// Signatory returns the Signatory from the Address, based on the Signature. If
// the Address is unsigned, then the empty Signatory is returned.
func (addr *Address) Signatory() (id.Signatory, error) {
	buf := make([]byte, surge.SizeHintU8+surge.SizeHintString(addr.Value)+surge.SizeHintU64)
	return addr.SignatoryWithBuffer(buf)
}

// SignatoryWithBuffer returns the Signatory from the Address, based on the
// Signature. If the Address is unsigned, then the empty Signatory is returned.
// It uses a Buffer for all marshaling to allow for buffer re-use.
func (addr *Address) SignatoryWithBuffer(buf []byte) (id.Signatory, error) {
	// Check whether or not the Address is unsigned.
	if addr.Signature.Equal(&id.Signature{}) {
		return id.Signatory{}, nil
	}

	// If the Address is signed, extract the Signatory and return it.
	hash, err := NewAddressHashWithBuffer(addr.Protocol, addr.Value, addr.Nonce, buf)
	if err != nil {
		return id.Signatory{}, fmt.Errorf("hashing address: %v", err)
	}
	pubKey, err := crypto.SigToPub(hash[:], addr.Signature[:])
	if err != nil {
		return id.Signatory{}, fmt.Errorf("identifying address signature: %v", err)
	}
	return id.NewSignatory((*id.PubKey)(pubKey)), nil
}

// String returns a human-readable representation of the Address. The string
// representation is safe for use in URLs and filenames.
func (addr Address) String() string {
	protocol := ""
	switch addr.Protocol {
	case TCP:
		protocol = "tcp"
	case UDP:
		protocol = "udp"
	case WebSocket:
		protocol = "ws"
	}
	return fmt.Sprintf("/%v/%v/%v/%v", protocol, addr.Value, addr.Nonce, addr.Signature)
}

// Equal compares two Addressees. Returns true if they are the same, otherwise
// returns false.
func (addr *Address) Equal(other *Address) bool {
	return addr.Protocol == other.Protocol &&
		addr.Value == other.Value &&
		addr.Nonce == other.Nonce &&
		addr.Signature.Equal(&other.Signature)
}

// DecodeString into a wire-compatible Address.
func DecodeString(addr string) (Address, error) {
	// Remove any leading slashes.
	if addr[:1] == "/" {
		addr = addr[1:]
	}

	addrParts := strings.Split(addr, "/")
	if len(addrParts) != 4 {
		return Address{}, fmt.Errorf("decoding string: invalid format=%v", addr)
	}
	var protocol uint8
	switch addrParts[0] {
	case "tcp":
		protocol = TCP
	case "udp":
		protocol = UDP
	case "ws":
		protocol = WebSocket
	}
	value := addrParts[1]
	nonce, err := strconv.ParseUint(addrParts[2], 10, 64)
	if err != nil {
		return Address{}, err
	}
	var sig id.Signature
	sigBytes, err := base64.RawURLEncoding.DecodeString(addrParts[3])
	if err != nil {
		return Address{}, err
	}
	copy(sig[:], sigBytes)
	return Address{
		Protocol:  protocol,
		Value:     value,
		Nonce:     nonce,
		Signature: sig,
	}, nil
}
