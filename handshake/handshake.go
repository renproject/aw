package handshake

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
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
	sessionCreator protocol.SessionCreator
}

func New(signVerifier protocol.SignVerifier, sessionCreator protocol.SessionCreator) Handshaker {
	if signVerifier == nil {
		panic("invariant violation: signVerifier cannot be nil")
	}
	if sessionCreator == nil {
		panic("invariant violation: sessionCreator cannot be nil")
	}
	return &handshaker{signVerifier: signVerifier, sessionCreator: sessionCreator}
}

func (hs *handshaker) Handshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	buf := new(bytes.Buffer)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	// Write initiator's public key
	if err := writePubKey(buf, rsaKey.PublicKey); err != nil {
		return nil, err
	}
	if err := hs.signAndAppendSignature(buf, rw); err != nil {
		return nil, err
	}

	// Read responder's public key and challenge
	challenger, err := hs.verifyAndStripSignature(rw, buf)
	if err != nil {
		return nil, err
	}
	challenge, err := readChallenge(buf, rsaKey)
	if err != nil {
		return nil, err
	}
	responderPubKey, err := readPubKey(buf)
	if err != nil {
		return nil, err
	}

	// Write the challenge reply
	if err := writeChallenge(buf, challenge, &responderPubKey); err != nil {
		return nil, err
	}
	if err := hs.signAndAppendSignature(buf, rw); err != nil {
		return nil, err
	}

	return hs.sessionCreator.Create(challenger, challenge), nil
}

func (hs *handshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	buf := new(bytes.Buffer)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	// Read initiator's public key
	initiator, err := hs.verifyAndStripSignature(rw, buf)
	if err != nil {
		return nil, err
	}
	initiatorPubKey, err := readPubKey(buf)
	if err != nil {
		return nil, err
	}

	// Create a challenge
	challengeLen := hs.sessionCreator.SecretLength()
	challenge := make([]byte, challengeLen)
	if n, err := rand.Read(challenge); n != challengeLen || err != nil {
		return nil, fmt.Errorf("failed to generate challenge: [%d != %d]: %v", n, challengeLen, err)
	}

	// Write challenge and responder's public key and challenge
	if err := writeChallenge(buf, challenge, &initiatorPubKey); err != nil {
		return nil, err
	}
	if err := writePubKey(buf, rsaKey.PublicKey); err != nil {
		return nil, err
	}
	if err := hs.signAndAppendSignature(buf, rw); err != nil {
		return nil, err
	}

	// Read initiator's challenge reply
	challengeResponder, err := hs.verifyAndStripSignature(rw, buf)
	if err != nil {
		return nil, err
	}

	if !challengeResponder.Equal(initiator) {
		return nil, fmt.Errorf("initiator and challenge responder are not the same %s != %s", initiator, challengeResponder)
	}

	replyChallenge, err := readChallenge(buf, rsaKey)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(challenge, replyChallenge) {
		return nil, fmt.Errorf("initiator failed the challenge %s != %s", base64.StdEncoding.EncodeToString(challenge[:]), base64.StdEncoding.EncodeToString(replyChallenge[:]))
	}

	return hs.sessionCreator.Create(initiator, challenge), nil
}

func writePubKey(w io.Writer, pubKey rsa.PublicKey) error {
	if err := binary.Write(w, binary.LittleEndian, int64(pubKey.E)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(pubKey.N.Bytes()))); err != nil {
		return err
	}
	if n, err := w.Write(pubKey.N.Bytes()); n != len(pubKey.N.Bytes()) || err != nil {
		return fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, len(pubKey.N.Bytes()), err)
	}
	return nil
}

func readPubKey(r io.Reader) (rsa.PublicKey, error) {
	pubKey := rsa.PublicKey{}
	var E int64
	if err := binary.Read(r, binary.LittleEndian, &E); err != nil {
		return pubKey, err
	}
	pubKey.E = int(E)
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return pubKey, err
	}
	pubKeyBytes := make([]byte, size)
	if n, err := r.Read(pubKeyBytes); uint64(n) != size || err != nil {
		return pubKey, fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, size, err)
	}
	pubKey.N = new(big.Int).SetBytes(pubKeyBytes)
	return pubKey, nil
}

func writeChallenge(w io.Writer, challenge []byte, pubKey *rsa.PublicKey) error {
	encryptedChallenge, err := rsa.EncryptPKCS1v15(rand.Reader, pubKey, challenge)
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(encryptedChallenge))); err != nil {
		return err
	}
	if n, err := w.Write(encryptedChallenge); n != len(encryptedChallenge) || err != nil {
		return fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, len(encryptedChallenge), err)
	}
	return nil
}

func readChallenge(r io.Reader, privKey *rsa.PrivateKey) ([]byte, error) {
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return []byte{}, err
	}
	encryptedChallenge := make([]byte, size)
	if n, err := r.Read(encryptedChallenge); uint64(n) != size || err != nil {
		return []byte{}, fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, size, err)
	}
	challengeBytes, err := rsa.DecryptPKCS1v15(rand.Reader, privKey, encryptedChallenge)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to decrypt the challenge [%d != 32]: %v", len(challengeBytes), err)
	}
	return challengeBytes, nil
}

func (hs *handshaker) signAndAppendSignature(r io.Reader, w io.Writer) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	n := len(data)

	hash := hs.signVerifier.Hash(data)
	sig, err := hs.signVerifier.Sign(hash)
	if err != nil {
		return err
	}
	sigLen := int(hs.signVerifier.SigLength())
	if err := binary.Write(w, binary.LittleEndian, uint64(n+sigLen)); err != nil {
		return err
	}
	if wn, err := w.Write(append(data, sig...)); wn != n+sigLen || err != nil {
		return fmt.Errorf("failed to add the signature [%d != %d]: %v", wn, n+sigLen, err)
	}
	return nil
}

func (hs *handshaker) verifyAndStripSignature(r io.Reader, w io.Writer) (protocol.PeerID, error) {
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, err
	}
	data := make([]byte, size)
	n, err := r.Read(data)
	if err != nil && uint64(n) != size {
		return nil, fmt.Errorf("failed to read [%d != %d]: %v", uint64(n), size, err)
	}
	sigLen := hs.signVerifier.SigLength()
	hash := hs.signVerifier.Hash(data[:size-sigLen])
	peerID, err := hs.signVerifier.Verify(hash, data[size-sigLen:])
	if err != nil {
		return peerID, err
	}
	if wn, err := w.Write(data[:size-sigLen]); uint64(wn) != size-sigLen || err != nil {
		return peerID, fmt.Errorf("failed to strip the signature [%d != %d]: %v", wn, n+int(sigLen), err)
	}
	return peerID, nil
}
