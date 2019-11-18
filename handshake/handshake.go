//  1.(Client) Generate a new RSA key ClientRSA.
//  2.(Client) Convert the public key of ClientRSA to bytes and write to server.
//  3.(Client) Sign the hash of RSA public key bytes and write to server.
//  4.(Server) Read signature from client and verify the signature.
//  5.(Server) Generate a new session key ServerSessionKey
//  6.(Server) Encrypt the ServerSessionKey with ClientRSA and write to client
//  7.(Server) Sign the hash of ServerSessionKey and write to client.
//  8.(Client) Read and decrypt ServerSessionKey and verify the signature.
//  9.(Client) Sign the hash of ServerSessionKey and write to server.
// 10.(Server) Read signature and verify it to confirm client has received the ServerSessionKey.
// 11.(Server) Generate a new RSA key ServerRSA.
// 12.(Server) Convert the public key of ServerRSA to bytes and write to client.
// 13.(Server) Sign the hash of public key of ServerRSA and write to client.
// 14.(Client) Read signature from server and verify the signature.
// 15.(Client) Generate a new session key ClientSessionKey
// 16.(Client) Encrypt the ClientSessionKey with ServerRSA and write to server
// 17.(Client) Sign the hash of ClientSessionKey and write to server.
// 18.(Server) Read and decrypt ClientSessionKey and verify the signature.
// 19.(Server) Sign the hash of ClientSessionKey and write to client.
// 20.(Client) Read signature and verify it to confirm server has received the ClientSessionKey.
// 21.(Client & Server) Xor the ClientSessionKey and ServerSessionKey to get the actual session key

package handshake

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"

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
	return &handshaker{signVerifier: signVerifier, sessionManager: sessionManager}
}

func (hs *handshaker) Handshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot generate rsa.PrivateKey"))
	}

	// Write our RSA public key with our signature.
	pubKeyBytes, err := pubKeyToBytes(&privKey.PublicKey)
	if err != nil {
		return nil, err
	}
	pubKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(pubKeyBytes))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign rsa.PublicKey: %v", err))
	}
	if err := write(rw, pubKeyBytes); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey to io.Writer: %v", err)
	}
	if err := write(rw, pubKeySig); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey signature to io.Writer: %v", err)
	}

	// Read the session key and verify the remote signature.
	remoteSessionKey, err := readEncrypted(rw, privKey)
	if err != nil {
		return nil, fmt.Errorf("error reading encrypted session key from io.Reader: %v", err)
	}
	localSessionKey := hs.sessionManager.NewSessionKey()
	if len(localSessionKey) != len(remoteSessionKey) {
		return nil, fmt.Errorf("error verifying session key: expected session key len=%v, got session key len=%v", len(localSessionKey), len(remoteSessionKey))
	}
	remoteSessionKeySig, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading session key signature from io.Reader: %v", err)
	}
	remotePeerID, err := hs.signVerifier.Verify(hs.signVerifier.Hash(remoteSessionKey), remoteSessionKeySig)
	if err != nil {
		return nil, fmt.Errorf("error verifying session key: %v", err)
	}

	// Write the session key with our signature.
	remoteSessionKeySig, err = hs.signVerifier.Sign(hs.signVerifier.Hash(remoteSessionKey))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign session key: %v", err))
	}
	if err := write(rw, remoteSessionKeySig); err != nil {
		return nil, fmt.Errorf("error writing session key signature to io.Writer: %v", err)
	}

	// Read the remote RSA public key and verify the signature.
	remotePubKeyBytes, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey from io.Reader: %v", err)
	}
	remotePubKey, err := pubKeyFromBytes(remotePubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling rsa.PublicKey: %v", err)
	}
	remotePubKeySig, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey signature from io.Reader: %v", err)
	}
	remotePeerID2, err := hs.signVerifier.Verify(hs.signVerifier.Hash(remotePubKeyBytes), remotePubKeySig)
	if err != nil {
		return nil, fmt.Errorf("error verifying rsa.PublicKey: %v", err)
	}
	if !remotePeerID.Equal(remotePeerID2) {
		return nil, fmt.Errorf("error verifying session key: expected peer=%v, got peer=%v", remotePeerID, remotePeerID2)
	}

	// Write our own session key with our signature.
	if err := writeEncrypted(rw, localSessionKey, &remotePubKey); err != nil {
		return nil, fmt.Errorf("error writing session key to io.Writer: %v", err)
	}
	localSessionKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(localSessionKey))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign session key: %v", err))
	}
	if err := write(rw, localSessionKeySig); err != nil {
		return nil, fmt.Errorf("error writing session key signature to io.Writer: %v", err)
	}

	// Read the remote session key signature.
	remoteSessionKeySig, err = read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading session key signature from io.Reader: %v", err)
	}
	remotePeerID2, err = hs.signVerifier.Verify(hs.signVerifier.Hash(localSessionKey), remoteSessionKeySig)
	if err != nil {
		return nil, fmt.Errorf("error verifying session key: %v", err)
	}
	if !remotePeerID.Equal(remotePeerID2) {
		return nil, fmt.Errorf("error verifying session key: expected peer=%v, got peer=%v", remotePeerID, remotePeerID2)
	}

	// Build the shared session
	sessionKey := xorSessionKeys(localSessionKey, remoteSessionKey)
	return hs.sessionManager.NewSession(remotePeerID, sessionKey), nil
}

func (hs *handshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	// Read the remote RSA public key and verify the signature.
	remotePubKeyBytes, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey from io.Reader: %v", err)
	}
	remotePubKey, err := pubKeyFromBytes(remotePubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling rsa.PublicKey: %v", err)
	}
	remotePubKeySig, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey signature from io.Reader: %v", err)
	}
	remotePeerID, err := hs.signVerifier.Verify(hs.signVerifier.Hash(remotePubKeyBytes), remotePubKeySig)
	if err != nil {
		return nil, fmt.Errorf("error verifying rsa.PublicKey: %v", err)
	}

	// Write the session key with our signature.
	localSessionKey := hs.sessionManager.NewSessionKey()
	if err := writeEncrypted(rw, localSessionKey, &remotePubKey); err != nil {
		return nil, fmt.Errorf("error writing session key to io.Writer: %v", err)
	}
	localSessionKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(localSessionKey))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign session key: %v", err))
	}
	if err := write(rw, localSessionKeySig); err != nil {
		return nil, fmt.Errorf("error writing session key signature to io.Writer: %v", err)
	}

	// Read the remote session key signature.
	remoteSessionKeySig, err := read(rw)
	if err != nil {
		return nil, err
	}
	remotePeerID2, err := hs.signVerifier.Verify(hs.signVerifier.Hash(localSessionKey), remoteSessionKeySig)
	if err != nil {
		return nil, err
	}
	if !remotePeerID.Equal(remotePeerID2) {
		return nil, fmt.Errorf("bad handshake: expected peer=%v, got peer=%v", remotePeerID, remotePeerID2)
	}

	// Write our RSA public key with our signature.
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot generate rsa.PrivateKey"))
	}
	pubKeyBytes, err := pubKeyToBytes(&privKey.PublicKey)
	if err != nil {
		return nil, err
	}
	pubKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(pubKeyBytes))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign rsa.PublicKey: %v", err))
	}
	if err := write(rw, pubKeyBytes); err != nil {
		return nil, err
	}
	if err := write(rw, pubKeySig); err != nil {
		return nil, err
	}

	// Read the session key and verify the remote signature.
	remoteSessionKey, err := readEncrypted(rw, privKey)
	if err != nil {
		return nil, err
	}
	if len(localSessionKey) != len(remoteSessionKey) {
		return nil, fmt.Errorf("bad handshake: expected session key len=%v, got session key len=%v", len(localSessionKey), len(remoteSessionKey))
	}
	remoteSessionKeySig, err = read(rw)
	if err != nil {
		return nil, err
	}
	remotePeerID2, err = hs.signVerifier.Verify(hs.signVerifier.Hash(remoteSessionKey), remoteSessionKeySig)
	if err != nil {
		return nil, err
	}
	if !remotePeerID.Equal(remotePeerID2) {
		return nil, fmt.Errorf("bad handshake: expected peer=%v, got peer=%v", remotePeerID, remotePeerID2)
	}
	remoteSessionKeySig, err = hs.signVerifier.Sign(hs.signVerifier.Hash(remoteSessionKey))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign session key: %v", err))
	}
	if err := write(rw, remoteSessionKeySig); err != nil {
		return nil, err
	}

	// Build the shared session
	sessionKey := xorSessionKeys(localSessionKey, remoteSessionKey)
	return hs.sessionManager.NewSession(remotePeerID, sessionKey), nil
}

// write data to an `io.Writer` with a prefixed length.
func write(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(len(data))); err != nil {
		return fmt.Errorf("error writing data len=%v: %v", len(data), err)
	}
	if err := binary.Write(w, binary.LittleEndian, data); err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	return nil
}

// read data from an `io.Reader` with a prefixed length.
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

// writeEncrypted data to an `io.Writer` with a prefixed length.
func writeEncrypted(w io.Writer, data []byte, pubKey *rsa.PublicKey) error {
	encryptedData, err := rsa.EncryptPKCS1v15(rand.Reader, pubKey, data)
	if err != nil {
		return err
	}
	return write(w, encryptedData)
}

// readEncrypted data from an `io.Reader` with a prefixed length. Returns the
// data after decryption.
func readEncrypted(r io.Reader, privKey *rsa.PrivateKey) ([]byte, error) {
	encryptedData, err := read(r)
	if err != nil {
		return nil, err
	}
	return rsa.DecryptPKCS1v15(rand.Reader, privKey, encryptedData)
}

func pubKeyToBytes(pubKey *rsa.PublicKey) ([]byte, error) {
	buf := new(bytes.Buffer)

	// E
	if err := binary.Write(buf, binary.LittleEndian, int64(pubKey.E)); err != nil {
		return buf.Bytes(), fmt.Errorf("error writing rsa.PublicKey: %v", err)
	}

	// N
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(pubKey.N.Bytes()))); err != nil {
		return buf.Bytes(), fmt.Errorf("error writing rsa.PublicKey: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, pubKey.N.Bytes()); err != nil {
		return buf.Bytes(), fmt.Errorf("error writing rsa.PublicKey: %v", err)
	}

	return buf.Bytes(), nil
}

func pubKeyFromBytes(data []byte) (rsa.PublicKey, error) {
	pubKey := rsa.PublicKey{}
	r := bytes.NewBuffer(data)

	// E
	e := int64(0)
	if err := binary.Read(r, binary.LittleEndian, &e); err != nil {
		return pubKey, fmt.Errorf("error reading rsa.PublicKey: %v", err)
	}
	pubKey.E = int(e)

	// N
	pubKeyLen := uint64(0)
	if err := binary.Read(r, binary.LittleEndian, &pubKeyLen); err != nil {
		return pubKey, fmt.Errorf("error reading rsa.PublicKey: %v", err)
	}
	pubKeyData := make([]byte, pubKeyLen)
	if err := binary.Read(r, binary.LittleEndian, &pubKeyData); err != nil {
		return pubKey, fmt.Errorf("error reading rsa.PublicKey: %v", err)
	}
	pubKey.N = new(big.Int).SetBytes(pubKeyData)

	return pubKey, nil
}

func xorSessionKeys(key1, key2 []byte) []byte {
	sessionKey := []byte{}
	for i := 0; i < len(key1); i++ {
		sessionKey = append(sessionKey, key1[i]^key2[i])
	}
	return sessionKey
}
