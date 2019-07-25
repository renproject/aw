package handshake

import (
	"context"
	"crypto/rsa"
	"io"

	"github.com/renproject/aw/protocol"
)

type HandShaker interface {
	OnConnect(ctx context.Context, rw io.ReadWriter) error
	OnReceive(ctx context.Context, rw io.ReadWriter) error
	OnChallengeAccept(ctx context.Context, rw io.ReadWriter) error
	OnChallengeResponse(ctx context.Context, rw io.ReadWriter) error
}

type handShaker struct {
	rsaPrivKey     *rsa.PrivateKey
	signerVerifier protocol.SignerVerifier
}

func NewHandShaker(signerVerifier protocol.SignerVerifier) HandShaker {
	return &handShaker{signerVerifier: signerVerifier}
}

func (sh *handShaker) OnConnect(ctx context.Context, rw io.ReadWriter) error {
	// privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	// if err != nil {
	// 	panic(err)
	// }
	// sh.rsaPrivKey = privKey
	// pubKeyBytes := serializePublicKey(privKey.PublicKey)
	// hash := sha3.Sum256(pubKeyBytes)
	// sig, err := sh.signerVerifier.Sign(hash[:])
	// if err != nil {
	// 	panic(err)
	// }
	// return NewMessage(append(pubKeyBytes, sig...))
	return nil
}

func (sh *handShaker) OnReceive(ctx context.Context, rw io.ReadWriter) error {
	return nil
}
func (sh *handShaker) OnChallengeAccept(ctx context.Context, rw io.ReadWriter) error {
	return nil
}
func (sh *handShaker) OnChallengeResponse(ctx context.Context, rw io.ReadWriter) error {
	return nil
}

func serializePublicKey(pubKey rsa.PublicKey) []byte {
	return nil
}
