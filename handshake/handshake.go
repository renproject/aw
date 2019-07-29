package handshake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"

	"github.com/renproject/aw/protocol"
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
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	if err := writePubKey(rw, rsaKey.PublicKey); err != nil {
		return err
	}
	challenge, err := readChallenge(rw, rsaKey)
	if err != nil {
		return err
	}
	responderPubKey, err := readPubKey(rw)
	if err != nil {
		return err
	}
	if err := writeChallenge(rw, challenge, &responderPubKey); err != nil {
		return err
	}
	return nil
}

func (hs *handShaker) Respond(ctx context.Context, rw io.ReadWriter) error {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	initiatorPubKey, err := readPubKey(rw)
	if err != nil {
		return err
	}
	challenge := [32]byte{}
	rand.Read(challenge[:])
	if err := writeChallenge(rw, challenge, &initiatorPubKey); err != nil {
		return err
	}
	if err := writePubKey(rw, rsaKey.PublicKey); err != nil {
		return err
	}
	replyChallenge, err := readChallenge(rw, rsaKey)
	if err != nil {
		return err
	}
	if challenge != replyChallenge {
		return fmt.Errorf("initiator failed the challenge %s != %s", base64.StdEncoding.EncodeToString(challenge[:]), base64.StdEncoding.EncodeToString(replyChallenge[:]))
	}
	return nil
}

func writePubKey(rw io.ReadWriter, pubKey rsa.PublicKey) error {
	if err := binary.Write(rw, binary.LittleEndian, int64(pubKey.E)); err != nil {
		return err
	}
	if err := binary.Write(rw, binary.LittleEndian, uint64(len(pubKey.N.Bytes()))); err != nil {
		return err
	}
	if n, err := rw.Write(pubKey.N.Bytes()); n != len(pubKey.N.Bytes()) || err != nil {
		return fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, len(pubKey.N.Bytes()), err)
	}
	return nil
}

func readPubKey(rw io.ReadWriter) (rsa.PublicKey, error) {
	pubKey := rsa.PublicKey{}
	var E int64
	if err := binary.Read(rw, binary.LittleEndian, &E); err != nil {
		return pubKey, err
	}
	pubKey.E = int(E)
	var size uint64
	if err := binary.Read(rw, binary.LittleEndian, &size); err != nil {
		return pubKey, err
	}
	pubKeyBytes := make([]byte, size)
	if n, err := rw.Read(pubKeyBytes); uint64(n) != size || err != nil {
		return pubKey, fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, size, err)
	}
	pubKey.N = new(big.Int).SetBytes(pubKeyBytes)
	return pubKey, nil
}

func writeChallenge(rw io.ReadWriter, challenge [32]byte, pubKey *rsa.PublicKey) error {
	encryptedChallenge, err := rsa.EncryptPKCS1v15(rand.Reader, pubKey, challenge[:])
	if err != nil {
		return err
	}
	if err := binary.Write(rw, binary.LittleEndian, uint64(len(encryptedChallenge))); err != nil {
		return err
	}
	if n, err := rw.Write(encryptedChallenge); n != len(encryptedChallenge) || err != nil {
		return fmt.Errorf("failed to write the pubkey [%d != %d]: %v", n, len(encryptedChallenge), err)
	}
	return nil
}

func readChallenge(rw io.ReadWriter, privKey *rsa.PrivateKey) ([32]byte, error) {
	var size uint64
	if err := binary.Read(rw, binary.LittleEndian, &size); err != nil {
		return [32]byte{}, err
	}
	encryptedChallenge := make([]byte, size)
	if n, err := rw.Read(encryptedChallenge); uint64(n) != size || err != nil {
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
