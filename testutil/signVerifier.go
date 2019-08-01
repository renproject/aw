package testutil

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/renproject/aw/protocol"
	"golang.org/x/crypto/sha3"
)

type MockSignVerifier interface {
	protocol.SignVerifier
	ID() string
	Whitelist(id string)
}

type mockSignVerifier struct {
	privKey   *ecdsa.PrivateKey
	whitelist map[string]bool
}

func NewMockSignVerifier(whitelistIDs ...string) *mockSignVerifier {
	privKey, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}
	whitelist := map[string]bool{}
	for _, id := range whitelistIDs {
		whitelist[id] = true
	}
	return &mockSignVerifier{privKey, whitelist}
}

func (sv *mockSignVerifier) Sign(digest []byte) ([]byte, error) {
	return crypto.Sign(digest, sv.privKey)
}

func (sv *mockSignVerifier) Verify(digest, sig []byte) error {
	pubKey, err := crypto.SigToPub(digest, sig)
	if err != nil {
		return err
	}
	whitelisted, ok := sv.whitelist[crypto.PubkeyToAddress(*pubKey).String()]
	if whitelisted && ok {
		return nil
	}
	return fmt.Errorf("unauthenticated user")
}

func (sv *mockSignVerifier) ID() string {
	return crypto.PubkeyToAddress(sv.privKey.PublicKey).String()
}

func (sv *mockSignVerifier) Whitelist(id string) {
	sv.whitelist[id] = true
}

func (sv *mockSignVerifier) Hash(data []byte) []byte {
	hash := sha3.Sum256(data)
	return hash[:]
}

func (sv *mockSignVerifier) SigLength() uint64 {
	return 65
}
