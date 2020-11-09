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
	"github.com/renproject/aw/codec"
	"github.com/renproject/id"
)

const sizeOfSecretKey = 32
const sizeOfEncryptedSecretKey = 145 // 113-byte encryption header + 32-byte secret key

func ECIES(privKey *id.PrivKey, r *rand.Rand) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		// Channel for passing errors from the writing goroutine to the reading
		// goroutine (which has the ability to return the error).
		errCh := make(chan error, 1)

		// Channel for passing the remote pubkey to the writing goroutine (which
		// operates in parallel to the reading goroutine).
		remotePubKeyCh := make(chan id.PubKey, 1)
		defer close(remotePubKeyCh)

		// Channel for passing the session key to the writing goroutine (which
		// operates in parallel to the reading goroutine).
		remoteSecretKeyCh := make(chan []byte, 1)
		defer close(remoteSecretKeyCh)

		// A pointer to the pubKey contained in the privKey struct
		localPubKey := privKey.PubKey()

		// Generate a local secret key. We do it here, because it is needed by
		// the writing and reading goroutine.
		localSecretKey := [sizeOfSecretKey]byte{}
		if _, err := r.Read(localSecretKey[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("generate local secret key: %v", err)
		}

		// Begin background goroutine for writing information to the network
		// connection.
		go func() {
			defer close(errCh)

			// Write local pubkey so that the remote peer knows how to encrypt
			// its secret key and send it back to the local peer.
			xBuf := paddedTo32(localPubKeyPtr.X)
			yBuf := paddedTo32(localPubKeyPtr.Y)
			if _, err := conn.Write(xBuf[:]); err != nil {
				errCh <- fmt.Errorf("write local pubkey x: %v", err)
				return
			}
			if _, err := conn.Write(yBuf[:]); err != nil {
				errCh <- fmt.Errorf("write local pubkey y: %v", err)
				return
			}

			// Encrypt the local secret key using the remote pubkey and write
			// it to the remote peer.
			remotePubKey, ok := <-remotePubKeyCh
			if !ok {
				return
			}
			importedRemotePubKey := ecies.ImportECDSAPublic((*ecdsa.PublicKey)(&remotePubKey))
			encryptedLocalSecretKey, err := ecies.Encrypt(r, importedRemotePubKey, localSecretKey[:], nil, nil)
			if err != nil {
				errCh <- fmt.Errorf("encrypt local secret key: %v", err)
				return
			}
			if _, err := conn.Write(encryptedLocalSecretKey); err != nil {
				errCh <- fmt.Errorf("write local secret key: %v", err)
				return
			}

			// Encrypted the remote secret key using the remote pubkey and write
			// it to the remote peer. This allows the remote peer to verify that
			// the local peer does have access to the previous asserted local
			// pubkey.
			remoteSecretKey, ok := <-remoteSecretKeyCh
			if !ok {
				return
			}
			encryptedRemoteSecretKey, err := ecies.Encrypt(r, importedRemotePubKey, remoteSecretKey, nil, nil)
			if err != nil {
				errCh <- fmt.Errorf("encrypt remote secret key: %v", err)
				return
			}
			if _, err := conn.Write(encryptedRemoteSecretKey); err != nil {
				errCh <- fmt.Errorf("write remote secret key: %v", err)
				return
			}
		}()

		// Read the remote pubkey.
		remotePubKeyBuf := [64]byte{}
		if _, err := io.ReadFull(conn, remotePubKeyBuf[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("read remote pubkey: %v", err)
		}
		remotePubKey := id.PubKey{
			Curve: crypto.S256(),
			X:     new(big.Int).SetBytes(remotePubKeyBuf[:32]),
			Y:     new(big.Int).SetBytes(remotePubKeyBuf[32:]),
		}
		remotePubKeyCh <- remotePubKey

		// Read the encrypted remote secret key, and then decrypt it.
		encryptedRemoteSecretKey := [sizeOfEncryptedSecretKey]byte{}
		if _, err := io.ReadFull(conn, encryptedRemoteSecretKey[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("read remote secret key: %v", err)
		}
		remoteSecretKey, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(encryptedRemoteSecretKey[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("decrypt remote secret key: %v", err)
		}
		remoteSecretKeyCh <- remoteSecretKey

		// Read the encrypted local secret back from the remote peer. This
		// proves to the local peer that the remote peer has access to its
		// previously asserted pubkey.
		encryptedLocalSecretKeyCheck := [sizeOfEncryptedSecretKey]byte{}
		if _, err := io.ReadFull(conn, encryptedLocalSecretKeyCheck[:]); err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("read local secret key: %v", err)
		}
		localSecretKeyCheck, err := ecies.ImportECDSA((*ecdsa.PrivateKey)(privKey)).Decrypt(encryptedLocalSecretKeyCheck[:], nil, nil)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("decrypt local secret key: %v", err)
		}
		if !bytes.Equal(localSecretKey[:], localSecretKeyCheck[:]) {
			return nil, nil, id.Signatory{}, fmt.Errorf("check local secret key")
		}

		// Check whether or not that an error happened in the writing goroutine
		// (and wait for the writing goroutine to end).
		err, ok := <-errCh
		if ok {
			return nil, nil, id.Signatory{}, err
		}

		// Build the session key, and use this to build GCM encoders/decoders.
		sessionKey := [sizeOfSecretKey]byte{}
		for i := 0; i < sizeOfSecretKey; i++ {
			sessionKey[i] = localSecretKey[i] ^ remoteSecretKey[i]
		}

		self := id.NewSignatory(localPubKeyPtr)
		remote := id.NewSignatory(&remotePubKey)
		gcmSession, err := codec.NewGCMSession(sessionKey, self, remote)
		if err != nil {
			return nil, nil, id.Signatory{}, fmt.Errorf("establish gcm session: %v", err)
		}
		return codec.GCMEncoder(gcmSession, enc), codec.GCMDecoder(gcmSession, dec), remote, nil
	}
}

// paddedTo32 encodes a big integer as a big-endian into a 32-byte array. It
// will panic if the big integer is more than 32 bytes.
// Modified from:
// https://github.com/ethereum/go-ethereum/blob/master/common/math/big.go
// 17381ecc6695ea9c2d8e5ee0aee5cf70d59a301a
func paddedTo32(bigint *big.Int) [32]byte {
	if bigint.BitLen()/8 > 32 {
		panic(fmt.Sprintf("too big: expected n<32, got n=%v", bigint.BitLen()/8))
	}
	ret := [32]byte{}
	readBits(bigint, ret[:])
	return ret
}

// readBits encodes the absolute value of bigint as big-endian bytes. Callers
// must ensure that buf has enough space. If buf is too short the result will be
// incomplete.
// Modified from:
// https://github.com/ethereum/go-ethereum/blob/master/common/math/big.go
// 17381ecc6695ea9c2d8e5ee0aee5cf70d59a301a
func readBits(bigint *big.Int, buf []byte) {
	i := len(buf)
	for _, d := range bigint.Bits() {
		for j := 0; j < wordBytes && i > 0; j++ {
			i--
			buf[i] = byte(d)
			d >>= 8
		}
	}
}

const (
	// wordBits is the number of bits in a big word.
	wordBits = 32 << (uint64(^big.Word(0)) >> 63)
	// wordBytes is the number of bytes in a big word.
	wordBytes = wordBits / 8
)
