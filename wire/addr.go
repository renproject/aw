package wire

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"
	"io"

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

// NewAddressHash returns the Hash of an Address for signing by the peer.
func NewAddressHash(protocol uint8, value string, nonce uint64) (id.Hash, error) {
	buf := new(bytes.Buffer)
	buf.Grow(surge.SizeHint(protocol) + surge.SizeHint(value) + surge.SizeHint(nonce))
	return NewAddressHashWithBuffer(protocol, value, nonce, buf)
}

// NewAddressHashWithBuffer returns the Hash of an Address for signing by the
// peer. It uses a Buffer for all marshaling, and expected the caller to Reset
// the Buffer before/after calling this function.
func NewAddressHashWithBuffer(protocol uint8, value string, nonce uint64, buf *bytes.Buffer) (id.Hash, error) {
	buf.Reset()
	m, err := surge.Marshal(buf, protocol, surge.MaxBytes)
	if err != nil {
		return id.Hash{}, fmt.Errorf("marshaling protocol: %v", err)
	}
	if m, err = surge.Marshal(buf, value, m); err != nil {
		return id.Hash{}, fmt.Errorf("marshaling value: %v", err)
	}
	if m, err = surge.Marshal(buf, nonce, m); err != nil {
		return id.Hash{}, fmt.Errorf("marshaling nonce: %v", err)
	}
	return id.Hash(sha256.Sum256(buf.Bytes())), nil
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
	return surge.SizeHint(addr.Protocol) +
		surge.SizeHint(addr.Value) +
		surge.SizeHint(addr.Nonce) +
		surge.SizeHint(addr.Signature)
}

// Marshal this Address into binary.
func (addr Address) Marshal(w io.Writer, m int) (int, error) {
	m, err := surge.Marshal(w, addr.Protocol, m)
	if err != nil {
		return m, fmt.Errorf("marsahling timestamp: %v", err)
	}
	m, err = surge.Marshal(w, addr.Value, m)
	if err != nil {
		return m, fmt.Errorf("marsahling protocol: %v", err)
	}
	m, err = surge.Marshal(w, addr.Nonce, m)
	if err != nil {
		return m, fmt.Errorf("marsahling value: %v", err)
	}
	m, err = surge.Marshal(w, addr.Signature, m)
	if err != nil {
		return m, fmt.Errorf("marsahling signature: %v", err)
	}
	return m, nil
}

// Unmarshal from binary into this Address.
func (addr *Address) Unmarshal(r io.Reader, m int) (int, error) {
	m, err := surge.Unmarshal(r, &addr.Protocol, m)
	if err != nil {
		return m, fmt.Errorf("unmarsahling timestamp: %v", err)
	}
	m, err = surge.Unmarshal(r, &addr.Value, m)
	if err != nil {
		return m, fmt.Errorf("unmarsahling protocol: %v", err)
	}
	m, err = surge.Unmarshal(r, &addr.Nonce, m)
	if err != nil {
		return m, fmt.Errorf("unmarsahling value: %v", err)
	}
	m, err = surge.Unmarshal(r, &addr.Signature, m)
	if err != nil {
		return m, fmt.Errorf("unmarsahling signature: %v", err)
	}
	return m, nil
}

// Sign this Address and set its Signature.
func (addr *Address) Sign(privKey *id.PrivKey) error {
	buf := new(bytes.Buffer)
	buf.Grow(surge.SizeHint(addr.Nonce) + surge.SizeHint(addr.Protocol) + surge.SizeHint(addr.Value))
	return addr.SignWithBuffer(privKey, buf)
}

// SignWithBuffer will Sign the Address and set its Signature. It uses a Buffer
// for all marshaling, and expected the caller to Reset the Buffer before/after
// calling this method.
func (addr *Address) SignWithBuffer(privKey *id.PrivKey, buf *bytes.Buffer) error {
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
	buf := new(bytes.Buffer)
	buf.Grow(surge.SizeHint(addr.Nonce) + surge.SizeHint(addr.Protocol) + surge.SizeHint(addr.Value))
	return addr.VerifyWithBuffer(signatory, buf)
}

// VerifyWithBuffer will verify that the Address was signed by a specific
// Signatory. It uses a Buffer for all marshaling, and expected the caller to
// Reset the Buffer before/after calling this method.
func (addr *Address) VerifyWithBuffer(signatory id.Signatory, buf *bytes.Buffer) error {
	hash, err := NewAddressHashWithBuffer(addr.Protocol, addr.Value, addr.Nonce, buf)
	if err != nil {
		return fmt.Errorf("hashing address: %v", err)
	}
	verifiedPubKey, err := crypto.SigToPub(hash[:], addr.Signature[:])
	if err != nil {
		return fmt.Errorf("identifying address signature: %v", err)
	}
	verifiedSignatory := id.NewSignatory(verifiedPubKey)
	if !signatory.Equal(&verifiedSignatory) {
		return fmt.Errorf("verifying address signatory: expected=%v, got=%v", signatory, verifiedSignatory)
	}
	return nil
}

// Signatory returns the Signatory from the Address, based on the Signature. If
// the Address is unsigned, then the empty Signatory is returned.
func (addr *Address) Signatory() (id.Signatory, error) {
	buf := new(bytes.Buffer)
	buf.Grow(surge.SizeHint(addr.Nonce) + surge.SizeHint(addr.Protocol) + surge.SizeHint(addr.Value))
	return addr.SignatoryWithBuffer(buf)
}

// SignatoryWithBuffer returns the Signatory from the Address, based on the
// Signature. If the Address is unsigned, then the empty Signatory is returned.
// It uses a Buffer for all marshaling, and expected the caller to Reset the
// Buffer before/after calling this method.
func (addr *Address) SignatoryWithBuffer(buf *bytes.Buffer) (id.Signatory, error) {
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
	return id.NewSignatory(pubKey), nil
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
