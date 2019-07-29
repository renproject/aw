package protocol

type SignerVerifier interface {
	Sign(digest []byte) ([]byte, error)
	Verify(digest, sig []byte) error
}
