package handshake

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/ecies"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type ecdsaHandshaker struct {
	privKey  *ecdsa.PrivateKey
	verifier Verifier
	maxSize  int
}

// NewECDSA returns a new Handshaker that uses ECDSA to establish a connection
// that is authenticated and encrypted using GCM.
func NewECDSA(privKey *ecdsa.PrivateKey, verifier Verifier, maxSize int) Handshaker {
	return &ecdsaHandshaker{privKey, verifier, maxSize}
}

func (hs *ecdsaHandshaker) Handshake(ctx context.Context, rw io.ReadWriter) (Session, error) {
	// 1. Write self ecdsa public key and Signature of it.
	if err := hs.writePublicKey(rw, hs.privKey); err != nil {
		return nil, err
	}

	// 2. Read the remote ECDSA public key and verify the signature.
	remotePublicKey, err := hs.readPublicKey(rw)
	if err != nil {
		return nil, err
	}

	// 3. Generate a session key, encrypted with remote ECDSA key and write to server
	localSessionKey := [32]byte{}
	if _, err := rand.Read(localSessionKey[:]); err != nil {
		return nil, err
	}
	if err := hs.writeEncrypted(rw, localSessionKey[:], remotePublicKey); err != nil {
		return nil, err
	}

	// 4. Read and decrypt the session key from the server.
	remoteSessionKey, err := hs.readEncrypted(rw, hs.privKey)
	if err != nil {
		return nil, err
	}

	return NewGCMSession(rw, xorSessionKeys(localSessionKey[:], remoteSessionKey), hs.maxSize)
}

func (hs *ecdsaHandshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (Session, error) {
	// 1. Read the remote ECDSA public key and verify the signature.
	remotePublicKey, err := hs.readPublicKey(rw)
	if err != nil {
		return nil, err
	}

	// 2. Write self ecdsa public key and Signature of it.
	if err := hs.writePublicKey(rw, hs.privKey); err != nil {
		return nil, err
	}

	// 3. Read and decrypt the session key from the client.
	remoteSessionKey, err := hs.readEncrypted(rw, hs.privKey)
	if err != nil {
		return nil, err
	}

	// 4. Generate a session key and write to client
	localSessionKey := [32]byte{}
	if _, err := rand.Read(localSessionKey[:]); err != nil {
		return nil, err
	}
	if err := hs.writeEncrypted(rw, localSessionKey[:], remotePublicKey); err != nil {
		return nil, err
	}

	return NewGCMSession(rw, xorSessionKeys(localSessionKey[:], remoteSessionKey), hs.maxSize)
}

// Write the ecdsa public along with a signature of it through the io.Writer
func (hs *ecdsaHandshaker) writePublicKey(w io.Writer, key *ecdsa.PrivateKey) error {
	// Write public key.
	pubKey := key.PublicKey
	pubKeyData := crypto.FromECDSAPub(&pubKey)
	if err := write(w, pubKeyData); err != nil {
		return fmt.Errorf("error writing ecdsa.PublicKey to io.Writer: %v", err)
	}

	// Write signed signatory.
	signatory := id.NewSignatory(key.PublicKey)
	signature, err := crypto.Sign(signatory[:], hs.privKey)
	if err != nil {
		return fmt.Errorf("invariant violation: cannot sign ecdsa.publickey: %v", err)
	}
	if err := write(w, signature); err != nil {
		return fmt.Errorf("error writing ecdsa.PublicKey signature to io.Writer: %v", err)
	}
	return nil
}

// Unmarshal the read data to an ecdsa.PublicKey and verify the signature.
func (hs *ecdsaHandshaker) readPublicKey(r io.Reader) (*ecdsa.PublicKey, error) {
	// Read public key.
	pubKeyData, err := read(r)
	if err != nil {
		return nil, fmt.Errorf("error reading ecdsa.PublicKey from io.Reader: %v", err)
	}
	pubKey, err := crypto.UnmarshalPubkey(pubKeyData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling ecdsa PublicKey: %v", err)
	}

	// Read signed signatory.
	signatory := id.NewSignatory(*pubKey)
	signature, err := read(r)
	if err != nil {
		return nil, fmt.Errorf("error reading ecdsa.PublicKey signature from io.Reader: %v", err)
	}

	verifiedPubKey, err := crypto.SigToPub(signatory[:], signature)

	// Verify.
	if err != nil || !id.NewSignatory(*verifiedPubKey).Equal(signatory) {
		return nil, fmt.Errorf("error verifying ecdsa.PublicKey: %v", err)
	}

	if hs.verifier != nil && !hs.verifier.Verify(signatory) {
		return nil, fmt.Errorf("error verifying id.Signatory: %v", err)
	}
	return pubKey, nil
}

// encrypt the data with given public key and write the encrypted data through an io.Writer.
func (hs *ecdsaHandshaker) writeEncrypted(w io.Writer, data []byte, publicKey *ecdsa.PublicKey) error {
	data, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(publicKey), data, nil, nil)
	if err != nil {
		return fmt.Errorf("error encrypting session key: %v", err)
	}
	if err := write(w, data); err != nil {
		return fmt.Errorf("error writing ecdsa.PublicKey signature to io.Writer: %v", err)
	}
	return nil
}

// read data from the io.Reader and decrypted with the ecdsa.PrivateKey.
func (hs *ecdsaHandshaker) readEncrypted(r io.Reader, privateKey *ecdsa.PrivateKey) ([]byte, error) {
	encryptedSessionKey, err := read(r)
	if err != nil {
		return nil, fmt.Errorf("error reading ecdsa.PublicKey from io.Reader: %v", err)
	}
	eciesPrivateKey := ecies.ImportECDSA(privateKey)
	decryptedSessionKey, err := eciesPrivateKey.Decrypt(encryptedSessionKey, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error decrypting session key from server: %v", err)
	}

	return decryptedSessionKey, nil
}

func write(w io.Writer, data []byte) error {
	return surge.Marshal(data, w)
}

func read(r io.Reader) ([]byte, error) {
	data := []byte{}
	err := surge.Unmarshal(&data, r)
	return data, err
}

func xorSessionKeys(key1, key2 []byte) []byte {
	sessionKey := make([]byte, 0)
	for i := 0; i < len(key1); i++ {
		sessionKey = append(sessionKey, key1[i]^key2[i])
	}
	return sessionKey
}
