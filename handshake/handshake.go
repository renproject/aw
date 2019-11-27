package handshake

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/ecies"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	"github.com/renproject/aw/protocol"
)

type Handshaker interface {
	// Handshake with a remote server by initiating, and then interactively
	// completing, a handshake protocol. The remote server is accessed by
	// reading/writing to the `io.ReaderWriter`.
	Handshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error)

	// AcceptHandshake from a remote client by waiting for the initiation of,
	// and then interactively completing, a handshake protocol. The remote
	// client is accessed by reading/writing to the `io.ReaderWriter`.
	AcceptHandshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error)
}

type handshaker struct {
	signVerifier   protocol.SignVerifier
	sessionManager protocol.SessionManager
}

func New(signVerifier protocol.SignVerifier, sessionManager protocol.SessionManager) Handshaker {
	if signVerifier == nil {
		panic("invariant violation: SignVerifier cannot be nil")
	}
	if sessionManager == nil {
		panic("invariant violation: SessionManager cannot be nil")
	}
	return &handshaker{
		signVerifier:   signVerifier,
		sessionManager: sessionManager,
	}
}

func (hs *handshaker) Handshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	// 1. Write self ECDSA public key and Signature of it.
	localPrivateKey, err := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("error generating new ecdsa key : %v", err)
	}
	if err := hs.writePublicKey(rw, localPrivateKey); err != nil {
		return nil, err
	}

	// 2. Read the remote ECDSA public key and verify the signature.
	remotePublicKey, remotePeerID, err := hs.readPublicKey(rw)
	if err != nil {
		return nil, err
	}

	// 3. Generate a session key, encrypted with remote ECDSA key and write to server
	localSessionKey := hs.sessionManager.NewSessionKey()
	if err := hs.writeEncrypted(rw, localSessionKey, remotePublicKey); err != nil {
		return nil, err
	}

	// 4. Read and decrypt the session key from the server.
	remoteSessionKey, err := hs.readEncrypted(rw, localPrivateKey)
	if err != nil {
		return nil, err
	}

	return hs.sessionManager.NewSession(remotePeerID, xorSessionKeys(localSessionKey, remoteSessionKey)), nil
}

func (hs *handshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	// 1. Read the remote ECDSA public key and verify the signature.
	remotePublicKey, remotePeerID, err := hs.readPublicKey(rw)
	if err != nil {
		return nil, err
	}

	// 2. Write self ecdsa public key and Signature of it.
	localPrivateKey, err := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("error generating new ecdsa key : %v", err)
	}
	if err := hs.writePublicKey(rw, localPrivateKey); err != nil {
		return nil, err
	}

	// 3. Read and decrypt the session key from the server.
	remoteSessionKey, err := hs.readEncrypted(rw, localPrivateKey)
	if err != nil {
		return nil, err
	}

	// 4. Generate a session key and write to server
	localSessionKey := hs.sessionManager.NewSessionKey()
	if err := hs.writeEncrypted(rw, localSessionKey, remotePublicKey); err != nil {
		return nil, err
	}
	return hs.sessionManager.NewSession(remotePeerID, xorSessionKeys(localSessionKey, remoteSessionKey)), nil
}

// Write the ecdsa public along with a signature of it through the io.Writer
func (hs *handshaker) writePublicKey(w io.Writer, key *ecdsa.PrivateKey) error {
	localPublicKey := key.PublicKey
	localPublicKeyBytes := crypto.FromECDSAPub(&localPublicKey)
	if err := write(w, localPublicKeyBytes); err != nil {
		return fmt.Errorf("error writing ecdsa.PublicKey to io.Writer: %v", err)
	}
	pubKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(localPublicKeyBytes))
	if err != nil {
		return fmt.Errorf("invariant violation: cannot sign ecdsa.publickey: %v", err)
	}
	if err := write(w, pubKeySig); err != nil {
		return fmt.Errorf("error writing ecdsa.PublicKey signature to io.Writer: %v", err)
	}
	return nil
}

// Unmarshal the read data to an ecdsa.PublicKey and verify the signature.
func (hs *handshaker) readPublicKey(r io.Reader) (*ecdsa.PublicKey, protocol.PeerID, error) {
	remotePubKeyBytes, err := read(r)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading ecdsa.PublicKey from io.Reader: %v", err)
	}
	remotePublicKey, err := crypto.UnmarshalPubkey(remotePubKeyBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("error unmarshaling ecdsa PublicKey: %v", err)
	}
	remotePubKeySig, err := read(r)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading ecdsa.PublicKey signature from io.Reader: %v", err)
	}
	remotePeerID, err := hs.signVerifier.Verify(hs.signVerifier.Hash(remotePubKeyBytes), remotePubKeySig)
	if err != nil {
		return nil, nil, fmt.Errorf("error verifying ecdsa.PublicKey: %v", err)
	}
	return remotePublicKey, remotePeerID, nil
}

// encrypt the data with given public key and write the encrypted data through an io.Writer.
func (hs *handshaker) writeEncrypted(w io.Writer, data []byte, publicKey *ecdsa.PublicKey) error {
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
func (hs *handshaker) readEncrypted(r io.Reader, privateKey *ecdsa.PrivateKey) ([]byte, error) {
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
	if err := binary.Write(w, binary.LittleEndian, uint64(len(data))); err != nil {
		return fmt.Errorf("error writing data len=%v: %v", len(data), err)
	}
	if err := binary.Write(w, binary.LittleEndian, data); err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	return nil
}

func read(r io.Reader) ([]byte, error) {
	dataLen := uint64(0)
	if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
		return nil, fmt.Errorf("error reading data len=%v: %v", dataLen, err)
	}
	data := make([]byte, dataLen)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return data, fmt.Errorf("error reading data: %v", err)
	}
	return data, nil
}

func xorSessionKeys(key1, key2 []byte) []byte {
	sessionKey := make([]byte, 0)
	for i := 0; i < len(key1); i++ {
		sessionKey = append(sessionKey, key1[i]^key2[i])
	}
	return sessionKey
}
