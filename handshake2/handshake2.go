package handshake2

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	mathRand "math/rand"

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

type Encoder func(w io.Writer, buf []byte) (int, error)
type Decoder func(r io.Reader, buf []byte) (int, error)

func LengthPrefixEncoder(enc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		prefix := uint32(len(buf))
		prefixBytes := [4]byte{}
		binary.BigEndian.PutUint32(prefixBytes[:], prefix)
		if _, err := enc(w, prefixBytes[:]); err != nil {
			return 0, fmt.Errorf("encoding data length: %v", err)
		}
		n, err := enc(w, buf)
		if err != nil {
			return n, fmt.Errorf("encoding data: %v", err)
		}
		return n, nil
	}
}

func LengthPrefixDecoder(dec Decoder) Decoder {
	return func(r io.Reader, buf []byte) (int, error) {
		prefixBytes := [4]byte{}
		if _, err := dec(r, prefixBytes[:]); err != nil {
			return 0, fmt.Errorf("decoding data length: %v", err)
		}
		prefix := binary.BigEndian.Uint32(prefixBytes[:])
		if uint32(len(buf)) < prefix {
			return 0, fmt.Errorf("decoding data length: expected %v, got %v", len(buf), prefix)
		}
		n, err := dec(r, buf[:prefix])
		if err != nil {
			return n, fmt.Errorf("decoding data: %v", err)
		}
		return n, nil
	}
}

func PlainEncoder(w io.Writer, buf []byte) (int, error) {
	return w.Write(buf)
}

func PlainDecoder(r io.Reader, buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}

type GCMSession struct {
	gcm         cipher.AEAD
	nonceRand   *mathRand.Rand
	nonceBuffer []byte
}

func NewGCMSession(key [32]byte) (*GCMSession, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return &GCMSession{}, fmt.Errorf("creating aes cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return &GCMSession{}, fmt.Errorf("creating gcm cipher: %v", err)
	}
	return &GCMSession{
		gcm:         gcm,
		nonceRand:   mathRand.New(mathRand.NewSource(int64(binary.BigEndian.Uint64(key[:8])))),
		nonceBuffer: make([]byte, gcm.NonceSize()),
	}, nil
}

func GCMEncoder(session *GCMSession, enc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		_, err := session.nonceRand.Read(session.nonceBuffer)
		if err != nil {
			return 0, err
		}
		n, err := enc(w, session.gcm.Seal(nil, session.nonceBuffer, buf, nil))
		if err != nil {
			return n, fmt.Errorf("encoding sealed data: %v", err)
		}
		return n, nil
	}
}

func GCMDecoder(session *GCMSession, dec Decoder) Decoder {
	return func(r io.Reader, buf []byte) (int, error) {
		if _, err := session.nonceRand.Read(session.nonceBuffer); err != nil {
			return 0, err
		}
		decryptedBuf, err := session.gcm.Open(nil, session.nonceBuffer, buf, nil)
		if err != nil {
			return 0, fmt.Errorf("decoding GCM sealed data: %v", err)
		}
		n, err := dec(r, decryptedBuf)
		if err != nil {
			return n, fmt.Errorf("decoding data: %v", err)
		}
		return n, nil
	}
}

type Handshake func(rw io.ReadWriter, enc Encoder, dec Decoder) (Encoder, Decoder, error)

func InsecureHandshake() Handshake {
	return func(_ io.ReadWriter, enc Encoder, dec Decoder) (Encoder, Decoder, error) {
		if enc == nil {
			enc = func(w io.Writer, buf []byte) (int, error) { return 0, fmt.Errorf("empty encoder") }
		}
		if dec == nil {
			dec = func(w io.Reader, buf []byte) (int, error) { return 0, fmt.Errorf("empty decoder") }
		}
		return enc, dec, nil
	}
}

func ECIESClientHandshake(privKey *id.PrivKey, serverPubKey *id.PubKey, r *mathRand.Rand) Handshake {
	return func(rw io.ReadWriter, enc Encoder, dec Decoder) (Encoder, Decoder, error) {

		clientPubKey := privKey.PubKey()
		bytesX, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.X))
		if err != nil {
			return nil, nil, fmt.Errorf("client converting own Public key X coordinate to bytes: %v", err)
		}
		bytesY, err := surge.ToBinary(pack.NewU256FromInt(clientPubKey.Y))
		if err != nil {
			return nil, nil, fmt.Errorf("client converting own Public key Y coordinate to bytes: %v", err)
		}
		if nBytes, err := rw.Write(bytesX); err != nil || nBytes != len(bytesX) {
			return nil, nil, fmt.Errorf("client sending Public key X coordinate to server\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}
		if nBytes, err := rw.Write(bytesY); err != nil || nBytes != len(bytesY) {
			return nil, nil, fmt.Errorf("client sending Public key Y coordinate to server\n\tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}

		eSSecretKeyBuf := [encryptedKeySize]byte{}
		if _, err := io.ReadFull(rw, eSSecretKeyBuf[:]); err != nil {
			return nil, nil, fmt.Errorf("client receiving server secret key: %v", err)
		}

		sSecretKey, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eSSecretKeyBuf[:], nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("client decrypting server secret key: %v", err)
		}

		cSecretKey := [keySize]byte{}
		if _, err := r.Read(cSecretKey[:]); err != nil {
			return nil, nil, fmt.Errorf("client generating new secret key: %v", err)
		}

		message := [keySize * 2]byte{}
		copy(message[:keySize], sSecretKey[:])
		copy(message[keySize:], cSecretKey[:])

		eMessage, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic((*ecdsa.PublicKey)(serverPubKey)), message[:], nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("client encrypting own secret key: %v", err)
		}

		if nBytes, err := rw.Write(eMessage); err != nil || nBytes != len(eMessage) {
			return nil, nil, fmt.Errorf("client sending concatenated server secret key and own sercret key\n \tError: %v\n\tnumber of bytes written: %v", err, nBytes)
		}

		eCSecretKeyBuf := [encryptedKeySize]byte{}
		if _, err := io.ReadFull(rw, eCSecretKeyBuf[:]); err != nil {
			return nil, nil, fmt.Errorf("client receiving own secret key back from server: %v", err)
		}

		cSecretKeyEcho, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eCSecretKeyBuf[:], nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("client decrypting own secret key: %v", err)
		}

		if !bytes.Equal(cSecretKey[:], cSecretKeyEcho[:]) {
			return nil, nil, fmt.Errorf("client's secret key and echoed secret key from server don't match")
		}

		sessionKey := [keySize]byte{}
		copy(sessionKey[:], cSecretKey[:])
		for i := 0; i < keySize; i++ {
			sessionKey[i] ^= sSecretKey[i]
		}

		// fmt.Printf("Secret Session Key - %v\n", sessionKey)
		// fmt.Println()
		// fmt.Printf("Client Public Key - X : %x, Y : %x\n", clientPubKey.X.Bytes(), clientPubKey.Y.Bytes())
		// fmt.Println()

		gcmSession, err := NewGCMSession(sessionKey)
		if err != nil {
			return nil, nil, err
		}
		return GCMEncoder(gcmSession, enc), GCMDecoder(gcmSession, dec), nil
	}
}

func ECIESServerHandshake(privKey *id.PrivKey, r *mathRand.Rand) Handshake {
	return func(rw io.ReadWriter, enc Encoder, dec Decoder) (Encoder, Decoder, error) {

		clientPubKeyBuf := [keySize * 2]byte{}
		if _, err := io.ReadFull(rw, clientPubKeyBuf[:]); err != nil {
			return nil, nil,
				fmt.Errorf("server receiving client Public key: %v", err)
		}

		xAsU256 := pack.NewU256FromInt(big.NewInt(0))
		yAsU256 := pack.NewU256FromInt(big.NewInt(0))
		if err := surge.FromBinary(&xAsU256, clientPubKeyBuf[:keySize]); err != nil {
			return nil, nil,
				fmt.Errorf("server converting client Public key X coordinate from bytes: %v", err)
		}
		if err := surge.FromBinary(&yAsU256, clientPubKeyBuf[keySize:]); err != nil {
			return nil, nil,
				fmt.Errorf("server converting client Public key Y coordinate from bytes: %v", err)
		}

		clientPubKey := ecdsa.PublicKey{
			Curve: crypto.S256(),
			X:     xAsU256.Int(),
			Y:     yAsU256.Int(),
		}

		sSecretKey := [keySize]byte{}
		if _, err := r.Read(sSecretKey[:]); err != nil {
			return nil, nil,
				fmt.Errorf("server generating new secret key: %v", err)
		}
		eSSecretKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(&clientPubKey), sSecretKey[:], nil, nil)
		if err != nil {
			return nil, nil,
				fmt.Errorf("server encrypting secret key: %v", err)
		}

		if nBytes, err := rw.Write(eSSecretKey); err != nil || nBytes != len(eSSecretKey) {
			return nil, nil,
				fmt.Errorf("server sending encrypted secret key to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
		}

		eMessageBuf := [encryptionHeaderSize + keySize*2]byte{}
		if _, err := io.ReadFull(rw, eMessageBuf[:]); err != nil {
			return nil, nil,
				fmt.Errorf("server receiving concatenated own secret key and client secret key: %v", err)
		}
		message, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(eMessageBuf[:], nil, nil)
		if err != nil {
			return nil, nil,
				fmt.Errorf("server decrypting concatenated own secret key and client secret key: %v", err)
		}

		if !bytes.Equal(message[:keySize], sSecretKey[:]) {
			return nil, nil,
				fmt.Errorf("server's secret key and echoed secret key from client don't match")
		}

		eCSecretKey, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(&clientPubKey), message[keySize:], nil, nil)
		if err != nil {
			return nil, nil,
				fmt.Errorf("server encrypting client's secret key: %v", err)
		}

		if nBytes, err := rw.Write(eCSecretKey); err != nil || nBytes != len(eCSecretKey) {
			return nil, nil,
				fmt.Errorf("server sending encrypted client secret key back to client \n\tError: %v\n\t number of bytes written: %v", err, nBytes)
		}

		sessionKey := [keySize]byte{}
		copy(sessionKey[:], message[keySize:])
		for i := 0; i < keySize; i++ {
			sessionKey[i] ^= message[i]
		}

		// fmt.Printf("Secret Session Key - %v\n", sessionKey)
		// fmt.Println()
		// fmt.Printf("Client Public Key - X : %x, Y : %x\n", clientPubKey.X.Bytes(), clientPubKey.Y.Bytes())
		// fmt.Println()

		gcmSession, err := NewGCMSession(sessionKey)
		if err != nil {
			return nil, nil, err
		}
		return GCMEncoder(gcmSession, enc), GCMDecoder(gcmSession, dec), nil
	}
}
