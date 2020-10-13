package handshake2

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	mathRand "math/rand"
	"net"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/ecies"
	"github.com/renproject/id"
	"github.com/renproject/pack"
	"github.com/renproject/surge"
)

func CHandshake(conn net.Conn, privKey *id.PrivKey, serverPubKey *id.PubKey, r *mathRand.Rand) ([]byte, error) {
	clientPubKey := privKey.PubKey()
	bytesX, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.X))
	if err != nil {
		return nil, fmt.Errorf("client converting own Public key X coordinate to bytes: %v", err)
	}
	bytesY, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.Y))
	if err != nil {
		return nil, fmt.Errorf("client converting own Public key Y coordinate to bytes: %v", err)
	}
	if nBytes, err := conn.Write(bytesX); err != nil || nBytes != len(bytesX) {
		return nil, fmt.Errorf("client sending Public key X coordinate to server\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
	}
	if nBytes, err := conn.Write(bytesY); err != nil || nBytes != len(bytesY) {
		return nil, fmt.Errorf("client sending Public key Y coordinate to server\n\tError: %v\n\tnumber of bytes written: %v", err, nBytes)
	}

	eSSecretKeyBuf := [encryptedKeySize]byte{}
	if _, err := io.ReadFull(conn, eSSecretKeyBuf[:]); err != nil {
		return nil, fmt.Errorf("client receiving server secret key: %v", err)
	}

	sSecretKey, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eSSecretKeyBuf[:], nil, nil)
	if err != nil {
		return nil, fmt.Errorf("client decrypting server secret key: %v", err)
	}

	cSecretKey := [keySize]byte{}
	if _, err := r.Read(cSecretKey[:]); err != nil {
		return nil, fmt.Errorf("client generating new secret key: %v", err)
	}

	message := [keySize * 2]byte{}
	copy(message[:keySize], sSecretKey[:])
	copy(message[keySize:], cSecretKey[:])

	eMessage, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic((*ecdsa.PublicKey)(serverPubKey)), message[:], nil, nil)
	if err != nil {
		return nil, fmt.Errorf("client encrypting own secret key: %v", err)
	}

	if nBytes, err := conn.Write(eMessage); err != nil || nBytes != len(eMessage) {
		return nil, fmt.Errorf("client sending concatenated server secret key and own sercret key\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
	}

	eCSecretKeyBuf := [encryptedKeySize]byte{}
	if _, err := io.ReadFull(conn, eCSecretKeyBuf[:]); err != nil {
		return nil, fmt.Errorf("client receiving own secret key back from server: %v", err)
	}

	cSecretKeyEcho, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eCSecretKeyBuf[:], nil, nil)
	if err != nil {
		return nil, fmt.Errorf("client decrypting own secret key: %v", err)
	}

	if !bytes.Equal(cSecretKey[:], cSecretKeyEcho[:]) {
		return nil, fmt.Errorf("client's secret key and echoed secret key from server don't match")
	}

	sessionKey := [keySize]byte{}
	copy(sessionKey[:], cSecretKey[:])
	for i := 0; i < keySize; i++ {
		sessionKey[i] ^= sSecretKey[i]
	}

	return sessionKey[:], nil
}

func AcceptHandshake(conn net.Conn, privKey *id.PrivKey, r *mathRand.Rand) ([]byte, id.PubKey, error) {

	clientPubKeyBuf := [keySize * 2]byte{}
	if _, err := io.ReadFull(conn, clientPubKeyBuf[:]); err != nil {
		return nil, id.PubKey{Curve: nil, X: nil, Y: nil},
			fmt.Errorf("server receiving client Public key: %v", err)
	}

	xAsU256 := pack.NewU256FromInt(big.NewInt(0))
	yAsU256 := pack.NewU256FromInt(big.NewInt(0))
	if err := surge.FromBinary(&xAsU256, clientPubKeyBuf[:keySize]); err != nil {
		return nil, id.PubKey{Curve: nil, X: nil, Y: nil},
			fmt.Errorf("server converting client Public key X coordinate from bytes: %v", err)
	}
	if err := surge.FromBinary(&yAsU256, clientPubKeyBuf[keySize:]); err != nil {
		return nil, id.PubKey{Curve: nil, X: nil, Y: nil},
			fmt.Errorf("server converting client Public key Y coordinate from bytes: %v", err)
	}

	clientPubKey := ecdsa.PublicKey{
		Curve: crypto.S256(),
		X:     xAsU256.Int(),
		Y:     yAsU256.Int(),
	}

	sSecretKey := [keySize]byte{}
	if _, err := r.Read(sSecretKey[:]); err != nil {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server generating new secret key: %v", err)
	}
	eSSecretKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(&clientPubKey), sSecretKey[:], nil, nil)
	if err != nil {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server encrypting secret key: %v", err)
	}

	if nBytes, err := conn.Write(eSSecretKey); err != nil || nBytes != len(eSSecretKey) {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server sending encrypted secret key to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
	}

	eMessageBuf := [encryptionHeaderSize + keySize*2]byte{}
	if _, err := io.ReadFull(conn, eMessageBuf[:]); err != nil {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server receiving concatenated own secret key and client secret key: %v", err)
	}
	message, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eMessageBuf[:], nil, nil)
	if err != nil {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server decrypting concatenated own secret key and client secret key: %v", err)
	}

	if !bytes.Equal(message[:keySize], sSecretKey[:]) {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server's secret key and echoed secret key from client don't match")
	}

	eCSecretKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(&clientPubKey), message[keySize:], nil, nil)
	if err != nil {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server encrypting client's secret key: %v", err)
	}

	if nBytes, err := conn.Write(eCSecretKey); err != nil || nBytes != len(eCSecretKey) {
		return nil, (id.PubKey)(clientPubKey),
			fmt.Errorf("server sending encrypted client secret key back to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
	}

	sessionKey := make([]byte, 32)
	copy(sessionKey, message[32:64])
	for i := 0; i < 32; i++ {
		sessionKey[i] ^= message[i]
	}

	return sessionKey, id.PubKey(clientPubKey), nil
}
