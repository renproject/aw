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
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"

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
	return &handshaker{signVerifier: signVerifier, sessionManager: sessionManager}
}

func (hs *handshaker) Handshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	localPrivateKey, err := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("error generating new ecdsa key : %v", err)
	}

	// Write self PeerID and Signature of it.
	localPublicKey := localPrivateKey.PublicKey
	localPublicKeyBytes := crypto.FromECDSAPub(&localPublicKey)
	if err := write(rw, localPublicKeyBytes); err != nil {
		return nil, fmt.Errorf("error writing ecdsa.PublicKey to io.Writer: %v", err)
	}
	pubKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(localPublicKeyBytes))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign ecdsa.publickey: %v", err))
	}
	if err := write(rw, pubKeySig); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey signature to io.Writer: %v", err)
	}

	// Read the remote ECDSA public key and verify the signature.
	remotePubKeyBytes, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey from io.Reader: %v", err)
	}
	remotePublicKey, err := crypto.UnmarshalPubkey(remotePubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling ecdsa PublicKey: %v", err)
	}
	remotePubKeySig, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey signature from io.Reader: %v", err)
	}
	remotePeerID, err := hs.signVerifier.Verify(hs.signVerifier.Hash(remotePubKeyBytes), remotePubKeySig)
	if err != nil {
		return nil, fmt.Errorf("error verifying rsa.PublicKey: %v", err)
	}

	// Generate a session key and write to server
	sessionKey := hs.sessionManager.NewSessionKey()
	data, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(remotePublicKey), sessionKey, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error encrypting session key: %v", err)
	}
	if err := write(rw, data); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey signature to io.Writer: %v", err)
	}

	// Read and decrypt the session to confirm server receive it.
	encryptedSessionKey, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey from io.Reader: %v", err)
	}
	eciesPrivateKey := ecies.ImportECDSA(localPrivateKey)
	decryptedSessionKey, err := eciesPrivateKey.Decrypt(encryptedSessionKey, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error decrypting session key from server: %v", err)
	}
	if !bytes.Equal(decryptedSessionKey, sessionKey) {
		return nil, fmt.Errorf("server sends wrong session key back")
	}

	return hs.sessionManager.NewSession(remotePeerID, sessionKey), nil
}

func (hs *handshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	// Read the remote ECDSA public key and verify the signature.
	remotePubKeyBytes, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey from io.Reader: %v", err)
	}
	remotePublicKey, err := crypto.UnmarshalPubkey(remotePubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling remote ecdsa PublicKey: %v", err)
	}
	remotePubKeySig, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey signature from io.Reader: %v", err)
	}
	remotePeerID, err := hs.signVerifier.Verify(hs.signVerifier.Hash(remotePubKeyBytes), remotePubKeySig)
	if err != nil {
		return nil, fmt.Errorf("error verifying rsa.PublicKey: %v", err)
	}

	// Write self PeerID and Signature of it.
	localPrivateKey, err := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("error generating new ecdsa key : %v", err)
	}
	localPublicKey := localPrivateKey.PublicKey
	localPublicKeyBytes := crypto.FromECDSAPub(&localPublicKey)
	if err := write(rw, localPublicKeyBytes); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey to io.Writer: %v", err)
	}
	pubKeySig, err := hs.signVerifier.Sign(hs.signVerifier.Hash(localPublicKeyBytes))
	if err != nil {
		panic(fmt.Errorf("invariant violation: cannot sign rsa.PublicKey: %v", err))
	}
	if err := write(rw, pubKeySig); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey signature to io.Writer: %v", err)
	}

	// Read and decrypt session key from the client
	encryptedSessionKey, err := read(rw)
	if err != nil {
		return nil, fmt.Errorf("error reading rsa.PublicKey from io.Reader: %v", err)
	}
	eciesPrivateKey := ecies.ImportECDSA(localPrivateKey)
	sessionKey, err := eciesPrivateKey.Decrypt(encryptedSessionKey, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error decrypting session key from client: %v", err)
	}

	// Encrypt the session key with client's publick key and write to client
	data, err := ecies.Encrypt(rand.Reader, ecies.ImportECDSAPublic(remotePublicKey), sessionKey, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error encrypting session key: %v", err)
	}
	if err := write(rw, data); err != nil {
		return nil, fmt.Errorf("error writing rsa.PublicKey signature to io.Writer: %v", err)
	}

	// fixme : peerID shoud not be nil
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
