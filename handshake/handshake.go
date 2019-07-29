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
	"golang.org/x/crypto/sha3"
)

type HandShaker interface {
	Initiate(ctx context.Context, rw io.ReadWriter) error
	Respond(ctx context.Context, rw io.ReadWriter) error
}

type handShaker struct {
	signVerifier protocol.SignVerifier
}

func NewHandShaker(signVerifier protocol.SignVerifier) HandShaker {
	return &handShaker{signVerifier: signVerifier}
}

func (hs *handShaker) Initiate(ctx context.Context, rw io.ReadWriter) error {
	buf := new(bytes.Buffer)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// Write initiator's public key
	if err := writePubKey(buf, rsaKey.PublicKey); err != nil {
		return err
	}
	if err := hs.sign(buf, rw); err != nil {
		return err
	}

	// Read responder's public key and challenge
	if err := hs.verify(rw, buf); err != nil {
		return err
	}
	challenge, err := readChallenge(buf, rsaKey)
	if err != nil {
		return err
	}
	responderPubKey, err := readPubKey(buf)
	if err != nil {
		return err
	}

	// Write the challenge reply
	if err := writeChallenge(buf, challenge, &responderPubKey); err != nil {
		return err
	}
	if err := hs.sign(buf, rw); err != nil {
		return err
	}

	return nil
}

func (hs *handShaker) Respond(ctx context.Context, rw io.ReadWriter) error {
	buf := new(bytes.Buffer)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// Read initiator's public key
	if err := hs.verify(rw, buf); err != nil {
		return err
	}
	initiatorPubKey, err := readPubKey(buf)
	if err != nil {
		return err
	}

	// Write challenge and responder's public key and challenge
	challenge := [32]byte{}
	rand.Read(challenge[:])
	if err := writeChallenge(buf, challenge, &initiatorPubKey); err != nil {
		return err
	}
	if err := writePubKey(buf, rsaKey.PublicKey); err != nil {
		return err
	}
	if err := hs.sign(buf, rw); err != nil {
		return err
	}

	// Read initiator's challenge reply
	if err := hs.verify(rw, buf); err != nil {
		return err
	}
	replyChallenge, err := readChallenge(buf, rsaKey)
	if err != nil {
		return err
	}

	if challenge != replyChallenge {
		return fmt.Errorf("initiator failed the challenge %s != %s", base64.StdEncoding.EncodeToString(challenge[:]), base64.StdEncoding.EncodeToString(replyChallenge[:]))
	}
	return nil
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

func writeChallenge(w io.Writer, challenge [32]byte, pubKey *rsa.PublicKey) error {
	encryptedChallenge, err := rsa.EncryptPKCS1v15(rand.Reader, pubKey, challenge[:])
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

func readChallenge(r io.Reader, privKey *rsa.PrivateKey) ([32]byte, error) {
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return [32]byte{}, err
	}
	encryptedChallenge := make([]byte, size)
	if n, err := r.Read(encryptedChallenge); uint64(n) != size || err != nil {
		return [32]byte{}, fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, size, err)
	}
	challengeBytes, err := rsa.DecryptPKCS1v15(rand.Reader, privKey, encryptedChallenge)
	if err != nil || len(challengeBytes) != 32 {
		return [32]byte{}, fmt.Errorf("failed to decrypt the challenge [%d != 32]: %v", len(challengeBytes), err)
	}
	challenge := [32]byte{}
	copy(challenge[:], challengeBytes)
	return challenge, nil
}

func (hs *handShaker) sign(r io.Reader, w io.Writer) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	n := len(data)

	hash := sha3.Sum256(data)
	sig, err := hs.signVerifier.Sign(hash[:])
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(n+65)); err != nil {
		return err
	}
	if wn, err := w.Write(append(data, sig...)); wn != n+65 || err != nil {
		return fmt.Errorf("failed to add the signature [%d != %d]: %v", wn, n+65, err)
	}
	return nil
}

func (hs *handShaker) verify(r io.Reader, w io.Writer) error {
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return err
	}
	data := make([]byte, size)
	n, err := r.Read(data)
	if err != nil && uint64(n) != size {
		return fmt.Errorf("failed to read [%d != %d]: %v", uint64(n), size, err)
	}
	hash := sha3.Sum256(data[:size-65])
	if err := hs.signVerifier.Verify(hash[:], data[size-65:]); err != nil {
		return err
	}
	if wn, err := w.Write(data[:size-65]); uint64(wn) != size-65 || err != nil {
		return fmt.Errorf("failed to strip the signature [%d != %d]: %v", wn, n+65, err)
	}
	return nil
}
