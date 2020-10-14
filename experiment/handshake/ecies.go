package handshake

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/ecies"
	"github.com/renproject/aw/experiment/codec"
	"github.com/renproject/id"
	"github.com/renproject/pack"
	"github.com/renproject/surge"
)

func ECIESClientHandshake(privKey *id.PrivKey, r *rand.Rand) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {

		serverPubKeyBuf := [keySize * 2]byte{}
		if _, err := io.ReadFull(conn, serverPubKeyBuf[:]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("clieant receiving server Public key: %v", err)
		}

		xAsU256 := pack.NewU256FromInt(big.NewInt(0))
		yAsU256 := pack.NewU256FromInt(big.NewInt(0))
		if err := surge.FromBinary(&xAsU256, serverPubKeyBuf[:keySize]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("client converting server Public key X coordinate from bytes: %v", err)
		}
		if err := surge.FromBinary(&yAsU256, serverPubKeyBuf[keySize:]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("client converting server Public key Y coordinate from bytes: %v", err)
		}

		serverPubKey := id.PubKey{
			Curve: crypto.S256(),
			X:     xAsU256.Int(),
			Y:     yAsU256.Int(),
		}

		clientPubKey := privKey.PubKey()
		bytesX, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.X))
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client converting own Public key X coordinate to bytes: %v", err)
		}
		bytesY, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.Y))
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client converting own Public key Y coordinate to bytes: %v", err)
		}
		if nBytes, err := conn.Write(bytesX); err != nil || nBytes != len(bytesX) {
			return nil, nil, id.Signatory{}, fmt.Errorf("client sending Public key X coordinate to server\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}
		if nBytes, err := conn.Write(bytesY); err != nil || nBytes != len(bytesY) {
			return nil, nil, id.Signatory{}, fmt.Errorf("client sending Public key Y coordinate to server\n\tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}

		eSSecretKeyBuf := [encryptedKeySize]byte{}
		if _, err := io.ReadFull(conn, eSSecretKeyBuf[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client receiving server secret key: %v", err)
		}

		sSecretKey, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eSSecretKeyBuf[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client decrypting server secret key: %v", err)
		}

		cSecretKey := [keySize]byte{}
		if _, err := r.Read(cSecretKey[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client generating new secret key: %v", err)
		}

		message := [keySize * 2]byte{}
		copy(message[:keySize], sSecretKey[:])
		copy(message[keySize:], cSecretKey[:])

		eMessage, err := ecies.Encrypt(r, ecies.ImportECDSAPublic((*ecdsa.PublicKey)(&serverPubKey)), message[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client encrypting own secret key: %v", err)
		}

		if nBytes, err := conn.Write(eMessage); err != nil || nBytes != len(eMessage) {
			return nil, nil, id.Signatory{}, fmt.Errorf("client sending concatenated server secret key and own sercret key\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}

		eCSecretKeyBuf := [encryptedKeySize]byte{}
		if _, err := io.ReadFull(conn, eCSecretKeyBuf[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client receiving own secret key back from server: %v", err)
		}

		cSecretKeyEcho, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eCSecretKeyBuf[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("client decrypting own secret key: %v", err)
		}

		if !bytes.Equal(cSecretKey[:], cSecretKeyEcho[:]) {
			return nil, nil, id.Signatory{}, fmt.Errorf("client's secret key and echoed secret key from server don't match")
		}

		sessionKey := [keySize]byte{}
		for i := 0; i < keySize; i++ {
			sessionKey[i] = cSecretKey[i] ^ sSecretKey[i]
		}
		gcmSession, err := codec.NewGCMSession(sessionKey)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("establishing gcm session: %v", err)
		}
		return codec.GCMEncoder(gcmSession, enc), codec.GCMDecoder(gcmSession, dec), id.NewSignatory(&serverPubKey), nil
	}
}

func ECIESServerHandshake(privKey *id.PrivKey, r *rand.Rand) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {

		serverPubKey := privKey.PubKey()
		bytesX, err := surge.ToBinary(pack.NewU256FromInt(serverPubKey.X))
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("server converting own Public key X coordinate to bytes: %v", err)
		}
		bytesY, err := surge.ToBinary(pack.NewU256FromInt(serverPubKey.Y))
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("server converting own Public key Y coordinate to bytes: %v", err)
		}
		if nBytes, err := conn.Write(bytesX); err != nil || nBytes != len(bytesX) {
			return nil, nil, id.Signatory{}, fmt.Errorf("server sending Public key X coordinate to server\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}
		if nBytes, err := conn.Write(bytesY); err != nil || nBytes != len(bytesY) {
			return nil, nil, id.Signatory{}, fmt.Errorf("server sending Public key Y coordinate to server\n\tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}

		clientPubKeyBuf := [keySize * 2]byte{}
		if _, err := io.ReadFull(conn, clientPubKeyBuf[:]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server receiving client Public key: %v", err)
		}

		xAsU256 := pack.NewU256FromInt(big.NewInt(0))
		yAsU256 := pack.NewU256FromInt(big.NewInt(0))
		if err := surge.FromBinary(&xAsU256, clientPubKeyBuf[:keySize]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server converting client Public key X coordinate from bytes: %v", err)
		}
		if err := surge.FromBinary(&yAsU256, clientPubKeyBuf[keySize:]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server converting client Public key Y coordinate from bytes: %v", err)
		}

		clientPubKey := id.PubKey{
			Curve: crypto.S256(),
			X:     xAsU256.Int(),
			Y:     yAsU256.Int(),
		}

		sSecretKey := [keySize]byte{}
		if _, err := r.Read(sSecretKey[:]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server generating new secret key: %v", err)
		}
		eSSecretKey, err := ecies.Encrypt(r, ecies.ImportECDSAPublic((*ecdsa.PublicKey)(&clientPubKey)), sSecretKey[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server encrypting secret key: %v", err)
		}

		if nBytes, err := conn.Write(eSSecretKey); err != nil || nBytes != len(eSSecretKey) {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server sending encrypted secret key to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
		}

		eMessageBuf := [encryptionHeaderSize + keySize*2]byte{}
		if _, err := io.ReadFull(conn, eMessageBuf[:]); err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server receiving concatenated own secret key and client secret key: %v", err)
		}
		message, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eMessageBuf[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server decrypting concatenated own secret key and client secret key: %v", err)
		}

		if !bytes.Equal(message[:keySize], sSecretKey[:]) {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server's secret key and echoed secret key from client don't match")
		}

		eCSecretKey, err := ecies.Encrypt(r, ecies.ImportECDSAPublic((*ecdsa.PublicKey)(&clientPubKey)), message[keySize:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server encrypting client's secret key: %v", err)
		}

		if nBytes, err := conn.Write(eCSecretKey); err != nil || nBytes != len(eCSecretKey) {
			return nil, nil, id.Signatory{},
				fmt.Errorf("server sending encrypted client secret key back to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
		}

		sessionKey := [keySize]byte{}
		for i := 0; i < keySize; i++ {
			sessionKey[i] = message[i] ^ message[keySize+i]
		}
		gcmSession, err := codec.NewGCMSession(sessionKey)
		if err != nil {
			return nil, nil, id.Signatory{}, err
		}
		return codec.GCMEncoder(gcmSession, enc), codec.GCMDecoder(gcmSession, dec), id.NewSignatory((*id.PubKey)(&clientPubKey)), nil
	}
}
