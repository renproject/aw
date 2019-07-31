package testutil

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/renproject/aw/protocol"
)

type MockSignerVerifier interface {
	protocol.SignerVerifier
	ID() string
	Whitelist(id string)
}

type mockSignerVerifier struct {
	privKey   *ecdsa.PrivateKey
	whitelist map[string]bool
}

func NewMockSignerVerifier(whitelistIDs ...string) *mockSignerVerifier {
	privKey, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}
	whitelist := map[string]bool{}
	for _, id := range whitelistIDs {
		whitelist[id] = true
	}
	return &mockSignerVerifier{privKey, whitelist}
}

func (sv *mockSignerVerifier) Sign(digest []byte) ([]byte, error) {
	return crypto.Sign(digest, sv.privKey)
}

func (sv *mockSignerVerifier) Verify(digest, sig []byte) error {
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

func (sv *mockSignerVerifier) ID() string {
	return crypto.PubkeyToAddress(sv.privKey.PublicKey).String()
}

func (sv *mockSignerVerifier) Whitelist(id string) {
	sv.whitelist[id] = true
}
