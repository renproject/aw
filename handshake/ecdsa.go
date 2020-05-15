package handshake

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/ecies"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

type ecdsaHandshaker struct {
	opts Options
}

// NewECDSA returns a new Handshaker that uses ECDSA to establish a connection
// that is authenticated and encrypted using GCM.
func NewECDSA(opts Options) Handshaker {
	return &ecdsaHandshaker{opts: opts}
}

func (handshaker *ecdsaHandshaker) Handshake(ctx context.Context, conn net.Conn) (Session, error) {
	defer conn.SetDeadline(time.Time{})

	//
	// 1
	//
	if err := conn.SetWriteDeadline(time.Now().Add(handshaker.opts.Timeout / 4)); err != nil {
		return nil, fmt.Errorf("setting deadline: %v", err)
	}
	if err := writePubKeyWithSignature(conn, &handshaker.opts.PrivKey.PublicKey, handshaker.opts.PrivKey); err != nil {
		return nil, fmt.Errorf("writing client pubkey with signature: %v", err)
	}

	//
	// 2
	//
	if err := conn.SetReadDeadline(time.Now().Add(handshaker.opts.Timeout / 4)); err != nil {
		return nil, fmt.Errorf("setting deadline: %v", err)
	}
	serverPubKey, serverSignatory, err := readPubKeyWithSignature(conn)
	if err != nil {
		return nil, fmt.Errorf("reading server pubkey with signature: %v", err)
	}
	if handshaker.opts.Filter != nil && !handshaker.opts.Filter.Filter(serverSignatory) {
		return nil, fmt.Errorf("filtering: bad server")
	}

	//
	// 3
	//
	if err := conn.SetWriteDeadline(time.Now().Add(handshaker.opts.Timeout / 4)); err != nil {
		return nil, fmt.Errorf("setting deadline: %v", err)
	}
	clientKey := [32]byte{}
	if _, err := rand.Read(clientKey[:]); err != nil {
		return nil, fmt.Errorf("generating client key: %v", err)
	}
	if err := encryptAndWriteKey(conn, clientKey[:], serverPubKey); err != nil {
		return nil, fmt.Errorf("encrypting and writing client key: %v", err)
	}

	//
	// 4
	//
	if err := conn.SetReadDeadline(time.Now().Add(handshaker.opts.Timeout / 4)); err != nil {
		return nil, fmt.Errorf("setting deadline: %v", err)
	}
	serverKey, err := readAndDecryptKey(conn, handshaker.opts.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("reading and decrypting server key: %v", err)
	}

	return NewGCMSession(xor(clientKey[:], serverKey), id.NewSignatory(serverPubKey))
}

func (handshaker *ecdsaHandshaker) AcceptHandshake(ctx context.Context, conn net.Conn) (Session, error) {
	//
	// 1
	//
	clientPubKey, clientSignatory, err := readPubKeyWithSignature(conn)
	if err != nil {
		return nil, fmt.Errorf("reading client pubkey with signature: %v", err)
	}
	if handshaker.opts.Filter != nil && !handshaker.opts.Filter.Filter(clientSignatory) {
		return nil, fmt.Errorf("filtering: bad signatory")
	}

	//
	// 2
	//
	err = writePubKeyWithSignature(conn, &handshaker.opts.PrivKey.PublicKey, handshaker.opts.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("writing server pubkey with signature: %v", err)
	}

	//
	// 3
	//
	clientKey, err := readAndDecryptKey(conn, handshaker.opts.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("reading and decrypting client key: %v", err)
	}

	//
	// 4
	//
	serverKey := [32]byte{}
	if _, err := rand.Read(serverKey[:]); err != nil {
		return nil, fmt.Errorf("generating server key: %v", err)
	}
	if err := encryptAndWriteKey(conn, serverKey[:], clientPubKey); err != nil {
		return nil, fmt.Errorf("writing server key: %v", err)
	}

	return NewGCMSession(xor(clientKey, serverKey[:]), id.NewSignatory(clientPubKey))
}

func encryptAndWriteKey(w io.Writer, localKey []byte, pubKey *ecdsa.PublicKey) error {
	encryptedKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(pubKey), localKey, nil, nil)
	if err != nil {
		return fmt.Errorf("encrypting key: %v", err)
	}
	if _, err := surge.Marshal(w, encryptedKey, surge.MaxBytes); err != nil {
		return fmt.Errorf("writing key: %v", err)
	}
	return nil
}

func readAndDecryptKey(r io.Reader, privKey *ecdsa.PrivateKey) ([]byte, error) {
	encryptedKey := []byte{}
	if _, err := surge.Unmarshal(r, &encryptedKey, surge.MaxBytes); err != nil {
		return nil, fmt.Errorf("reading key: %v", err)
	}
	eciesPrivKey := ecies.ImportECDSA(privKey)
	key, err := eciesPrivKey.Decrypt(encryptedKey[:], nil, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypting key: %v", err)
	}
	return key, nil
}

func writePubKeyWithSignature(w io.Writer, pubKey *ecdsa.PublicKey, signer *ecdsa.PrivateKey) error {
	compressedPubKey := crypto.CompressPubkey(pubKey)
	if _, err := surge.Marshal(w, compressedPubKey, surge.MaxBytes); err != nil {
		return fmt.Errorf("marshaling pubkey: %v", err)
	}
	signatory := id.NewSignatory(pubKey)
	rawSignature, err := crypto.Sign(signatory[:], signer)
	if err != nil {
		return fmt.Errorf("signing pubkey: %v", err)
	}
	signature := id.Signature{}
	copy(signature[:], rawSignature)
	if _, err := surge.Marshal(w, signature, surge.MaxBytes); err != nil {
		return fmt.Errorf("marshaling signature: %v", err)
	}
	return nil
}

func readPubKeyWithSignature(r io.Reader) (*ecdsa.PublicKey, id.Signatory, error) {
	compressedPubKey := []byte{}
	if _, err := surge.Unmarshal(r, &compressedPubKey, surge.MaxBytes); err != nil {
		return nil, id.Signatory{}, fmt.Errorf("unmarshaling pubkey: %v", err)
	}
	pubKey, err := crypto.DecompressPubkey(compressedPubKey)
	if err != nil {
		return nil, id.Signatory{}, fmt.Errorf("decompressing pubkey: %v", err)
	}
	signatory := id.NewSignatory(pubKey)
	signature := id.Signature{}
	if _, err := surge.Unmarshal(r, &signature, surge.MaxBytes); err != nil {
		return nil, id.Signatory{}, fmt.Errorf("unmarshaling signature: %v", err)
	}
	verifiedPubKey, err := crypto.SigToPub(signatory[:], signature[:])
	if err != nil {
		return nil, id.Signatory{}, fmt.Errorf("verifying pubkey: %v", err)
	}
	if !id.NewSignatory(verifiedPubKey).Equal(&signatory) {
		return nil, id.Signatory{}, fmt.Errorf("verifying signatory: %v", err)
	}
	return pubKey, signatory, nil
}

func xor(k1, k2 []byte) []byte {
	key := make([]byte, len(k1))
	for i := 0; i < len(k1); i++ {
		key[i] = k1[i] ^ k2[i]
	}
	return key
}
