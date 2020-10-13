package handshake3

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/ecies"
	"github.com/renproject/id"
	"github.com/renproject/pack"
	"github.com/renproject/surge"
)

const keySize = 32

const encryptionHeaderSize = 113

// encryptedKeySize specifiec the size in bytes of a single key encrypted using ECIES
const encryptedKeySize = encryptionHeaderSize + keySize

// Implementation of a parallel handshake
func HandshakeSender(conn net.Conn, keyPair *id.PrivKey) ([]byte, error) {
	messageBuffer := [encryptedKeySize]byte{}

	clientPubKey := keyPair.PubKey()
	bytesX, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.X))
	if err != nil {
		return nil, fmt.Errorf("Converting own Public key X coordinate to bytes: %v", err)
	}
	bytesY, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.Y))
	if err != nil {
		return nil, fmt.Errorf("Converting own Public key Y coordinate to bytes: %v", err)
	}
	if nBytes, err := conn.Write(bytesX); err != nil || nBytes != len(bytesX) {
		return nil, fmt.Errorf("Sending Public key X coordinate to server\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
	}
	if nBytes, err := conn.Write(bytesY); err != nil || nBytes != len(bytesY) {
		return nil, fmt.Errorf("Sending Public key Y coordinate to server\n\tError: %v\n\tnumber of bytes written: %v", err, nBytes)
	}

	eSecretKeyBuf := messageBuffer[:]
	if _, err := io.ReadFull(conn, eSecretKeyBuf); err != nil {
		return nil, fmt.Errorf("client receiving server secret key: %v", err)
	}

	secretKey, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(keyPair)).Decrypt(eSecretKeyBuf, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("client decrypting server secret key: %v", err)
	}

	secretKeyAsU256 := pack.NewU256FromInt(big.NewInt(0))
	if err := surge.FromBinary(&secretKeyAsU256, secretKey); err != nil {
		return nil, fmt.Errorf("server converting client Public key X coordinate from bytes: %v", err)
	}

	secretKeyPair := id.PrivKey{D: secretKeyAsU256.Int()}

	eSecretKeyEchoBuf := [encryptedKeySize]byte{}
	if _, err := io.ReadFull(conn, eSecretKeyEchoBuf[:]); err != nil {
		return nil, fmt.Errorf("client receiving server secret key: %v", err)
	}

	secretKeyEcho, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(&secretKeyPair)).Decrypt(eSecretKeyEchoBuf[:], nil, nil)
	if err != nil {
		return nil, fmt.Errorf("client decrypting server secret key: %v", err)
	}

	if !bytes.Equal(secretKey, secretKeyEcho) {
		return nil, fmt.Errorf("client's secret key and echoed secret key from server don't match")
	}

	return secretKeyEcho[:], nil
}

func HandshakeReceiver(conn net.Conn, keyPair *id.PrivKey) ([]byte, id.PubKey, error) {
	messageBuffer := [64]byte{}

	clientPubKeyBuf := messageBuffer[:64]
	if _, err := io.ReadFull(conn, clientPubKeyBuf); err != nil {
		return nil, id.PubKey{Curve: nil, X: nil, Y: nil},
			fmt.Errorf("server receiving client Public key: %v", err)
	}

	xAsU256 := pack.NewU256FromInt(big.NewInt(0))
	yAsU256 := pack.NewU256FromInt(big.NewInt(0))
	if err := surge.FromBinary(&xAsU256, clientPubKeyBuf[:32]); err != nil {
		return nil, id.PubKey{Curve: nil, X: nil, Y: nil},
			fmt.Errorf("server converting client Public key X coordinate from bytes: %v", err)
	}
	if err := surge.FromBinary(&yAsU256, clientPubKeyBuf[32:]); err != nil {
		return nil, id.PubKey{Curve: nil, X: nil, Y: nil},
			fmt.Errorf("server converting client Public key Y coordinate from bytes: %v", err)
	}

	pubKey := ecdsa.PublicKey{
		Curve: crypto.S256(),
		X:     xAsU256.Int(),
		Y:     yAsU256.Int(),
	}

	handshakeKeyPair := id.NewPrivKey()
	secretKey, err := surge.ToBinary(pack.NewU256FromInt(handshakeKeyPair.D))
	if err != nil {
		return nil, (id.PubKey)(pubKey), fmt.Errorf("Converting secret key from big int to bytes: %v", err)
	}
	eSecretKeyUsingPeerPubKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(&pubKey), secretKey, nil, nil)
	if err != nil {
		return nil, (id.PubKey)(pubKey),
			fmt.Errorf("server encrypting secret key: %v", err)
	}
	if nBytes, err := conn.Write(eSecretKeyUsingPeerPubKey); err != nil || nBytes != len(eSecretKeyUsingPeerPubKey) {
		return nil, (id.PubKey)(pubKey), fmt.Errorf("Sending Public key X coordinate to server\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
	}

	eSecretKeyUsingSecretKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic((*ecdsa.PublicKey)(handshakeKeyPair.PubKey())), secretKey, nil, nil)
	if err != nil {
		return nil, (id.PubKey)(pubKey),
			fmt.Errorf("server encrypting secret key: %v", err)
	}

	if nBytes, err := conn.Write(eSecretKeyUsingSecretKey); err != nil || nBytes != len(eSecretKeyUsingSecretKey) {
		return nil, (id.PubKey)(pubKey),
			fmt.Errorf("server sending encrypted secret key to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
	}

	return secretKey, (id.PubKey)(pubKey), err
}
